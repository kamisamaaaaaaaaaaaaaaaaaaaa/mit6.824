for i in {1..1000}
do
    go test -run 2A
    echo "Test 2A $i round passed."
done