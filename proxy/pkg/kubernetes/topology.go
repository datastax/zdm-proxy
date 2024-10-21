package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type TopologyEventType uint8

const (
	AddedEvent TopologyEventType = iota
	RemovedEvent
	StatusChangedEvent
)

type TopologyEvent struct {
	Type     TopologyEventType
	Host     net.IP
	StatusUp bool
}

type TopologyRegistry struct {
	clientset *kubernetes.Clientset

	serviceNamespace string
	serviceName      string

	local net.IP

	nodes     map[string]bool // Represents a set of node IPs and their status
	nodesLock sync.RWMutex

	eventChannels     map[chan<- TopologyEvent]struct{}
	eventChannelsLock sync.RWMutex
}

func NewTopologyRegistry(serviceName string) (*TopologyRegistry, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	currentNamespace, err := os.ReadFile(
		"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
	)
	if err != nil {
		return nil, err
	}

	localIPs, err := net.LookupIP(os.Getenv("HOSTNAME"))
	if err != nil {
		return nil, err
	}
	if len(localIPs) == 0 {
		return nil, errors.New("No local IP address found")
	}

	return &TopologyRegistry{
		clientset:        clientset,
		serviceNamespace: string(currentNamespace),
		serviceName:      serviceName,
		local:            localIPs[0],
		nodes:            make(map[string]bool),
		eventChannels:    make(map[chan<- TopologyEvent]struct{}),
	}, nil
}

func (tr *TopologyRegistry) Start(ctx context.Context) {
	go tr.runInformer(ctx)
}

func (tr *TopologyRegistry) Peers() []net.IP {
	tr.nodesLock.RLock()
	defer tr.nodesLock.RUnlock()

	log.Debugf("[TopologyRegistry] Nodes: %v", tr.nodes)

	var result []net.IP
	for host, statusUp := range tr.nodes {
		if statusUp && host != tr.Local().String() {
			result = append(result, net.ParseIP(host))
		}
	}

	log.Debugf("[TopologyRegistry] Peers result: %v", result)

	return result
}

func (tr *TopologyRegistry) Local() net.IP {
	return tr.local
}

func (tr *TopologyRegistry) Subscribe(eventChannel chan TopologyEvent) {
	tr.eventChannelsLock.Lock()
	defer tr.eventChannelsLock.Unlock()

	tr.eventChannels[eventChannel] = struct{}{}
}

func (tr *TopologyRegistry) Unsubscribe(eventChannel chan TopologyEvent) {
	tr.eventChannelsLock.Lock()
	defer tr.eventChannelsLock.Unlock()

	delete(tr.eventChannels, eventChannel)
}

func (tr *TopologyRegistry) broadcastEvents(events []TopologyEvent) {
	log.Infof("[TopologyRegistry] Broadcasting topology events.")
	log.Debugf("[TopologyRegistry] Broadcasting topology events: %v", events)

	tr.eventChannelsLock.RLock()
	defer tr.eventChannelsLock.RUnlock()

	for _, event := range events {
		for ch := range tr.eventChannels {
			ch <- event
		}
	}
}

func (tr *TopologyRegistry) runInformer(ctx context.Context) {
	informerFactory := informers.NewFilteredSharedInformerFactory(
		tr.clientset,
		5*time.Minute,
		tr.serviceNamespace,
		func(options *metav1.ListOptions) {
			options.LabelSelector = fmt.Sprintf(
				"kubernetes.io/service-name=%s",
				tr.serviceName,
			)
		},
	)

	informer := informerFactory.Discovery().V1().EndpointSlices().Informer()
	informer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				endpointSlice, ok := obj.(*discoveryv1.EndpointSlice)
				if ok {
					tr.handleNewEndpointSlice(endpointSlice)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oldEndpointSlice, okOld := oldObj.(*discoveryv1.EndpointSlice)
				newEndpointSlice, okNew := newObj.(*discoveryv1.EndpointSlice)
				if okOld && okNew {
					tr.handleModifiedEndpointSlice(oldEndpointSlice, newEndpointSlice)
				}
			},
			DeleteFunc: func(obj interface{}) {
				endpointSlice, ok := obj.(*discoveryv1.EndpointSlice)
				if ok {
					tr.handleRemovedEndpointSlice(endpointSlice)
				}
			},
		},
	)

	log.Infof("[TopologyRegistry] Starting EndpointSlice informer.")
	defer log.Infof("[TopologyRegistry] Informer stopped.")

	stopCh := make(chan struct{})
	defer close(stopCh)

	informer.Run(stopCh)

	<-ctx.Done()
}

func (tr *TopologyRegistry) handleNewEndpointSlice(
	endpointSlice *discoveryv1.EndpointSlice,
) {
	var events []TopologyEvent
	defer func() {
		tr.broadcastEvents(events)
	}()

	tr.nodesLock.Lock()
	defer tr.nodesLock.Unlock()

	for _, endpoint := range endpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		statusUp := *endpoint.Conditions.Ready
		tr.nodes[host] = statusUp
		events = append(events, TopologyEvent{
			Type:     AddedEvent,
			Host:     net.ParseIP(host),
			StatusUp: statusUp,
		})
	}
}

func (tr *TopologyRegistry) handleModifiedEndpointSlice(
	oldEndpointSlice, newEndpointSlice *discoveryv1.EndpointSlice,
) {
	var events []TopologyEvent
	defer func() {
		tr.broadcastEvents(events)
	}()

	oldMap := make(map[string]bool, len(oldEndpointSlice.Endpoints))
	for _, endpoint := range oldEndpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		statusUp := *endpoint.Conditions.Ready
		oldMap[host] = statusUp
	}

	newMap := make(map[string]bool, len(newEndpointSlice.Endpoints))
	for _, endpoint := range newEndpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		statusUp := *endpoint.Conditions.Ready
		newMap[host] = statusUp
	}

	tr.nodesLock.Lock()
	defer tr.nodesLock.Unlock()

	for _, endpoint := range oldEndpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		if newStatusUp, ok := newMap[host]; ok {
			oldStatusUp := *endpoint.Conditions.Ready
			if oldStatusUp == newStatusUp {
				continue
			}

			tr.nodes[host] = newStatusUp
			events = append(events, TopologyEvent{
				Type:     StatusChangedEvent,
				Host:     net.ParseIP(host),
				StatusUp: newStatusUp,
			})
		} else {
			delete(tr.nodes, host)
			events = append(events, TopologyEvent{
				Type:     RemovedEvent,
				Host:     net.ParseIP(host),
				StatusUp: false,
			})
		}
	}

	for _, endpoint := range newEndpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		if statusUp, ok := oldMap[host]; !ok {
			tr.nodes[host] = statusUp
			events = append(events, TopologyEvent{
				Type:     AddedEvent,
				Host:     net.ParseIP(host),
				StatusUp: statusUp,
			})
		}
	}
}

func (tr *TopologyRegistry) handleRemovedEndpointSlice(
	endpointSlice *discoveryv1.EndpointSlice,
) {
	var events []TopologyEvent
	defer func() {
		tr.broadcastEvents(events)
	}()

	tr.nodesLock.Lock()
	defer tr.nodesLock.Unlock()

	for _, endpoint := range endpointSlice.Endpoints {
		if len(endpoint.Addresses) == 0 || endpoint.Addresses[0] == "" {
			continue
		}

		host := endpoint.Addresses[0]
		delete(tr.nodes, host)
		events = append(events, TopologyEvent{
			Type:     RemovedEvent,
			Host:     net.ParseIP(host),
			StatusUp: false,
		})
	}
}
