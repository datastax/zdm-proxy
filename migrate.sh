echo "start"
go build -o ./migration/bin ./migration/main.go
./migration/bin
code=$?
echo $code
while [ $code -eq 100 ]; do
  echo "test"
  HARD_RESTART=true ./migration/bin
  code=$?
done