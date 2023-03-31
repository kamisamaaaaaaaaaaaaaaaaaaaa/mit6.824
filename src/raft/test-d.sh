for i in {1..1000}
do
    go test -run 2D
    echo "Test 2D $i round passed."
done