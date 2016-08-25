'use strict';

const QueryBuilder = require('./query-builder').QueryBuilder;
const cachedHandler = require('./redis-cache-handler');

/**
 * @param payload
 * @return {QueryBuilder}
 */
function newQuery(payload) {
    return new QueryBuilder(payload);
}

module.exports = newQuery;
module.exports.cachedHandler = cachedHandler;