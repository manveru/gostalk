gstlkd:
	docker build -t build-gostalkd .
	docker run build-gostalkd | docker build -t gostalkd -
