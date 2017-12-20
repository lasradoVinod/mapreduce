ip="localhost:50051
localhost:50052
localhost:50053
localhost:50054
localhost:50055
localhost:50056"

for ipAddr in $ip
do
	gnome-terminal -e "./mr_worker $ipAddr"
done
