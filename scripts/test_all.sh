#! /bin/sh
# ./test_all.sh <dataset>
if [ $# = 0 ]; then
echo "please input dest directory"
exit
fi

./test_one_pair.sh RAM NO $1
./test_one_pair.sh RAM CFL $1
./test_one_pair.sh RAM CBR $1
./test_one_pair.sh RAM CAP $1

./test_one_pair.sh DDFS NO $1
./test_one_pair.sh DDFS CFL $1
./test_one_pair.sh DDFS CBR $1
./test_one_pair.sh DDFS CAP $1

./test_one_pair.sh EXBIN NO $1
./test_one_pair.sh EXBIN CFL $1
./test_one_pair.sh EXBIN CBR $1
./test_one_pair.sh EXBIN CAP $1

./test_one_pair.sh SILO NO $1
./test_one_pair.sh SILO CFL $1
./test_one_pair.sh SILO CBR $1
./test_one_pair.sh SILO CAP $1

./test_one_pair.sh SPARSE NO $1
./test_one_pair.sh SPARSE CFL $1
./test_one_pair.sh SPARSE CBR $1
./test_one_pair.sh SPARSE CAP $1
