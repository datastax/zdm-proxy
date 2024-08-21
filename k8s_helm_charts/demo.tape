# record with: $ vhs demo.tape

Output zdm-k8s-ccm-astra.gif

Set FontSize 12
Set Width 800
Set Height 600

Type "ls"
Enter
Sleep 500ms

Type "cd zdm"
Enter
Sleep 500ms

Type "cp ~/Downloads/secure-connect-test.zip ./secure-connect-bundle-target.zip"
Enter
Sleep 200ms

Type "echo 'Change username and password of C* clusters in values.yaml'"
Enter
Sleep 1s

Type "echo 'Change contact point of origin cluster to host IP address of Minikube (host.minikube.internal), e.g. 192.168.65.254'"
Enter
Sleep 1s

Type "cd .."
Enter
Sleep 200ms

Type "ccm create demo -v 4.1.5 -n 1"
Enter
Sleep 2s

Type "ccm start"
Enter
Sleep 5s

Type "ccm updateconf authenticator:PasswordAuthenticator"
Enter
Sleep 1s

Type "ccm updateconf broadcast_rpc_address:192.168.65.254"
Enter
Sleep 1s

Type "ccm stop && ccm start"
Enter
Sleep 10s

Type "ccm node1 cqlsh -u cassandra -p cassandra"
Enter
Sleep 500ms

Type "CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'} AND durable_writes = true;"
Enter
Sleep 700ms

Type "CREATE ROLE 'KjaeRUazLmLjPYYYjfikZyIB' with SUPERUSER = true and LOGIN = true and PASSWORD = '1GDI7naKuFY.SSdWu4j,4OB79-sPtLDZAzr.Gjsb5_135n1SjJz+y1Zs6jKGdem7-XZWknhWJzHlSFrZ631U.3X1iloZt.ZNP971yHC4Dfi6o1qvbDKs0_zdUhZ_-KDF';"
Enter
Sleep 700ms

Type "exit"
Enter
Sleep 200ms

Type "kubectl create ns zdm-proxy"
Enter
Sleep 500ms

Type "helm -n zdm-proxy install zdm-proxy zdm"
Enter
Sleep 3s

Type "kubectl get pods -n zdm-proxy"
Enter
Sleep 500ms

Type "kubectl logs zdm-proxy-0 -n zdm-proxy"
Enter
Sleep 3s

Type "kubectl apply -f ./monitoring"
Enter
Sleep 2s

Type "kubectl get pods -n zdm-proxy"
Enter
Sleep 500ms

Type "echo 'Change username and password of C* in nosqlbench.yml'"
Enter
Sleep 1s

Type "kubectl apply -f ./nosqlbench"
Enter
Sleep 3s

Type "kubectl get pods -n zdm-proxy"
Enter
Sleep 5s

