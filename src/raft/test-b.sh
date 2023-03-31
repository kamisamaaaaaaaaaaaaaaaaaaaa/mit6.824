for i in {1..1000}
do
    go test -run 2B
    echo "Test 2B $i round passed."
done