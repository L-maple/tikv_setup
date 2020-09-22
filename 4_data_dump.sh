#! /bin/sh

codeDirName="recommendation-keti2-v9.1/"

echo "cp docker_generator.py to recommendation-keti2..."
cp docker_generator.py $codeDirName
echo "docker_generator.py has been removed..."

echo "cd into recommendation-keti2..."
cd $codeDirName
echo "executing the docker_generator.py..."
python docker_generator.py cn.edu.neu.tiger.tikv.data.CreateItemDataByTiKV

echo "execute mvn to generate jar package..."
mvn clean install


