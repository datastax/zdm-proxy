NAME   := datastax/cloudgate-proxy
TAG    := $$(git log -1 --pretty=%H)
IMG    := ${NAME}:${TAG}
LATEST := ${NAME}:latest
 
build:
	@docker build -t ${IMG} .
	@docker tag ${IMG} ${LATEST}
 
push:
	@docker push ${IMG}
	@docker push ${LATEST}
 
login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}