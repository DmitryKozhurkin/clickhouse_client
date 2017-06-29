'use strict';

const request   = require('request');
const Converter = require('csvtojson').Converter;

class ClickHouseClient {

	constructor (options) {
		this.host   = options.host || '127.0.0.1';
		this.port   = options.port || 8123;
		this.dbname = options.dbname;
		this.format = options.format || 'JSONEachRow';
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
		}, (err, resp, data) => {
			err = (resp && resp.statusCode !== 200) ? new Error(data) : err;
			callback(err, data);
		});
	}

	post (sql, callback) {
		this.query(sql, '', callback);
	}

	get (sql, callback) {
		this.query(sql, callback);
	}

	find (sql, callback) {
		sql = `${sql} FORMAT CSVWithNames`;
		this.query(sql, (err, data) => {
			if (err) {
				return callback(err);
			}
			new Converter({}).fromString(data, callback);
		});
	}

	findOne (sql, callback) {
		return this.find(sql, (err, data) => callback(err, data && data[0]));
	}

	insert (tablename, data, options = {}, callback) {
		if (typeof options === 'function') {
			callback = options;
			options = {};
		}

		const format = options.format || this.format;

		let sql = `INSERT INTO ${tablename} FORMAT ${format}`;
		let body = typeof data === 'string' ? data : JSON.stringify(data).slice(1, -1);
		this.query(sql, body, callback);
	}

}

module.exports = ClickHouseClient;