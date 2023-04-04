for i in {1..1000}
do
    go test -run 3B
    echo "------------------------Test 3B $i round passed---------------------------"
done