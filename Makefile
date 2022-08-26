NAME   := datastax/zdm-proxy
TAG    := $$(git log -1 --pretty=%H)
IMG    := ${NAME}:${TAG}
 
build:
	@docker build -t ${IMG} .
 
push:
	@docker push ${IMG}
	@echo "Pushed to docker hub: ${IMG}"

get_current_tag:
	@echo ${IMG}
 
login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
