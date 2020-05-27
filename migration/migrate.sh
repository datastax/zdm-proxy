go build -o ./migration/bin ./migration/main.go
./migration/bin
code=$?
while [ $code -eq 100 ]; do
  HARD_RESTART=true ./migration/bin
  code=$?
done