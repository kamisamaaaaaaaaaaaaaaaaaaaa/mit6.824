for i in {1..1000}
do
    go test -run 3A
    go test -run 3B
    echo "------------------------Test 3 $i round passed---------------------------"
done