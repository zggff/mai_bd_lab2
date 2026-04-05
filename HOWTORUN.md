## Как запустить:
запускает базы данных и копирует данные
```bash
docker compose up
```

### Трансформация в звезду
```bash
docker compose cp ./spark/star.py spark-master:/app/star.py
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  star.py
```

### Создание отчетов
```bash
docker compose cp ./spark/datamarts.py spark-master:/app/datamarts.py
docker compose exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  datamarts.py
```

### Запуск обоих
```bash
./run_spark.sh
```

