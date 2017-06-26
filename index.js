'use strict';

const request   = require('request');
const Converter = require('csvtojson').Converter;

class ClickHouseClient {

	constructor (options) {
		this.host   = options.host || '127.0.0.1';
		this.port   = options.port || 8123;
		this.dbname = options.dbname;
	}

	query (sql, body, callback) {
		if (typeof body === 'function') {
			callback = body;
			body     = null;
		}

		let method = body === null ? 'get' : 'post';

		request[method]({
			url  : `http://${this.host}:${this.port}/`,
			qs   : {
				query    : sql,
				database : this.dbname
			},
			body : body
		}, function(err, resp, data) {
			err = (resp && resp.statusCode !== 200) ? new Error(data) : err;
			callback(err, data);
		});
	}

	find (sql, callback) {
		sql = `${sql} FORMAT CSVWithNames`;
		this.query(sql, null, function(err, data) {
			if (err) {
				return callback(err);
			}
			new Converter({}).fromString(data, callback);
		});
	}

	findOne (sql, callback) {
		return this.find(sql, (err, data) => callback(err, data && data[0]));
	}

	insert (tablename, data, options, callback) {
		if (typeof options === 'function') {
			callback = options;
			options = {
				format: 'JSONEachRow'
			};
		}

		if (options.format !== 'JSONEachRow' && typeof data !== 'string') {
			options.format = 'JSONEachRow';
		}

		let sql = `INSERT INTO ${tablename} FORMAT ${options.format}`;
		let body = typeof data === 'string' ? data : JSON.stringify(data).slice(1, -1);
		this.query(sql, body, callback);
	}

}

module.exports = ClickHouseClient;