set -eo

FF_DEST_CPU=19

if pgrep -x "FrogFish" > /dev/null
then
	pkill FrogFish
fi

for nr_clients in 2 4 8 16;
do
	for scale in 32 320 3200;
	do
		cd ../DB
		rm nodes.dat log.dat

		echo "Starting database"

		taskset -c ${FF_DEST_CPU} ./FrogFish &
		FF_PID=$!
		sleep 20

		cd ../scripts

		echo "DB should be ready"

		echo "Generating configs for ${nr_clients} clients ${scale} series"
		python GenerateConfigs.py ${nr_clients} ${scale}

		for client in $(seq 1 ${nr_clients});
		do
			mv FrogFishClient-${client}.toml ../client/client-${client}/FrogFishClient.toml
		done

		PID=0

		cd ../client
		for client in $(seq 1 ${nr_clients});
		do 
			cd client-${client}
			taskset -c ${client} ./FrogFishClient &
			PID=$!
			cd ../
		done

		while pgrep -x "FrogFishClient" > /dev/null ;
		do
			sleep 5
		done

		for client in $(seq 1 ${nr_clients});
		do
			mv ./client-${client}/res.csv ../scripts/ff_${nr_clients}_client_${client}_${scale}_series.csv
		done

		echo "Done"
		pkill -INT FrogFish
		sleep 5
	done
done
