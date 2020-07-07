FROM ubuntu:20.04
COPY ./bin /opt/cloud-gate/bin

CMD /opt/cloud-gate/bin/cloud-gate.sh
