'use strict';

const QueryBuilder = require('./query-builder').QueryBuilder;
const $ = require('highland');
const _ = require('lodash');

function multiQueryHandler(handler, { discriminator = '_type'} = {}) {
    return function (query, reply) {
        let active;
        const queries = [].concat(query.payload);
        const stream = $(queries).flatMap(({ id, language, payload, fields, options, params, dataTypes }) => {
            active = new QueryBuilder(payload)
                .configure({ language, fields, options, dataTypes, handler })
                .build(params);
            return active.stream().tap(v => _.set(v, discriminator, id));
        });
        query.on('cancel', () => active && active.cancel());
        reply(stream).on('end', () => active && active.cancel());
    };
}

module.exports = multiQueryHandler;