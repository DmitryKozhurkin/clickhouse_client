```
const ClickHouseClient = require('clickhouse_client');

let clickhouseClient = new ClickHouseClient({
	host: "localhost",
	port: 8123,
	dbname: "database"
});

clickhouseClient.find('SELECT * FROM table_name LIMIT 10', (err, data) => {
	if (err) {
		return console.log(err);
	}

	console.log({data});
});
```