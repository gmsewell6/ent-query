'use strict';

const _ = require('lodash');
const QueryBuilder = require('./query-builder').QueryBuilder;
const cachedHandler = require('./redis-cache-handler');
const multiHandler = require('./multi-query-handler');

let CACHE_DEFAULTS = {};

/**
 * @param payload
 * @return {QueryBuilder}
 */
function newQuery(payload) {
    return new QueryBuilder(payload);
}

module.exports = newQuery;

module.exports.cachedHandler = function(handler, opts) {
    return cachedHandler(handler, _.defaults({}, opts, CACHE_DEFAULTS));
};

module.exports.multiHandler = function(handler, opts) {
    return multiHandler(handler, opts);
};

module.exports.CACHE_DEFAULTS = CACHE_DEFAULTS;