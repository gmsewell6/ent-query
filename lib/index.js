'use strict';

const QueryBuilder = require('./query-builder').QueryBuilder;
const cachedHandler = require('./redis-cache-handler');

exports.create = payload => new QueryBuilder(payload);
exports.cachedHandler = cachedHandler;