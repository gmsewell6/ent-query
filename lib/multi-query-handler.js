'use strict';

const QueryBuilder = require('./query-builder').QueryBuilder;
const $ = require('highland');
const _ = require('lodash');

function multiQueryHandler(handler, { discriminator = '_type'} = {}) {
    return function (query, reply) {
        let active;
        const queries = [].concat(query.payload);
        const stream = $(queries).flatMap(({ id, language, payload, options }) => {
            active = new QueryBuilder(payload)
                .language(language)
                .options(options)
                .handler(handler)
                .build();
            return active.stream().tap(v => _.set(v, discriminator, id));
        });
        query.on('cancel', () => active && active.cancel());
        reply(stream).on('end', () => active && active.cancel());
    };
}

module.exports = multiQueryHandler;