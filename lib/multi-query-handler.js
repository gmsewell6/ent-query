'use strict';

const QueryBuilder = require('./query-builder').QueryBuilder;
const $ = require('highland');
const _ = require('lodash');

function multiQueryHandler(handler, { discriminator = '_type', discriminatorLabel } = {}) {
    return function (query, reply) {
        let active;
        const queries = [].concat(query.payload);
        const stream = $(queries).flatMap(({ id, limit, language, payload, fields, options, params, dataTypes }) => {
            active = new QueryBuilder(payload)
                .configure({ limit, language, fields, options, dataTypes, handler })
                .through(s => s.tap(v => _.set(v, [ discriminator ], id)))
                .build(params);
            return active.stream();
        });
        query.on('cancel', () => active && active.cancel());
        reply(stream)
            .field(discriminator, fd => fd.position(0).label(discriminatorLabel))
            .on('end', () => active && active.cancel());
    };
}

module.exports = multiQueryHandler;