for i in {1..1000}
do
    go test -run 2C
    echo "Test 2C $i round passed."
done