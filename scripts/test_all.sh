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
./test_one_pair.sh RAM HBR $1
./test_one_pair.sh RAM HBR_CBR $1
./test_one_pair.sh RAM HBR_CAP $1

./test_one_pair.sh DDFS NO $1
./test_one_pair.sh DDFS CFL $1
./test_one_pair.sh DDFS CBR $1
./test_one_pair.sh DDFS CAP $1
./test_one_pair.sh DDFS HBR $1
./test_one_pair.sh DDFS HBR_CBR $1
./test_one_pair.sh DDFS HBR_CAP $1

./test_one_pair.sh EXBIN NO $1
./test_one_pair.sh EXBIN CFL $1
./test_one_pair.sh EXBIN CBR $1
./test_one_pair.sh EXBIN CAP $1
./test_one_pair.sh EXBIN HBR $1
./test_one_pair.sh EXBIN HBR_CBR $1
./test_one_pair.sh EXBIN HBR_CAP $1

./test_one_pair.sh SILO NO $1
./test_one_pair.sh SILO CFL $1
./test_one_pair.sh SILO CBR $1
./test_one_pair.sh SILO CAP $1
./test_one_pair.sh SILO HBR $1
./test_one_pair.sh SILO HBR_CBR $1
./test_one_pair.sh SILO HBR_CAP $1
