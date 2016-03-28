/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

// Fire me up!

/**
 * Module interaction with browsers.
 */
module.exports = {
    implements: 'browser-manager',
    inject: ['require(lodash)', 'require(socket.io)', 'require(apache-ignite)', 'agent-manager', 'configure']
};

module.exports.factory = (_, socketio, apacheIgnite, agentMgr, configure) => {
    const SqlFieldsQuery = apacheIgnite.SqlFieldsQuery;
    const ScanQuery = apacheIgnite.ScanQuery;

    return {
        attach: (server) => {
            const io = socketio(server);

            configure.socketio(io);

            io.sockets.on('connection', (socket) => {
                const user = socket.client.request.user;

                // Return available drivers to browser.
                socket.on('schemaImport:drivers', (cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.availableDrivers())
                        .then((drivers) => cb(null, drivers))
                        .catch((errMsg) => cb(errMsg));
                });

                // Return schemas from database to browser.
                socket.on('schemaImport:schemas', (preset, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const jdbcInfo = {user: preset.user, password: preset.password};

                            return agent.metadataSchemas(preset.jdbcDriverJar, preset.jdbcDriverClass, preset.jdbcUrl, jdbcInfo);
                        })
                        .then((schemas) => cb(null, schemas))
                        .catch((errMsg) => cb(errMsg));
                });

                // Return tables from database to browser.
                socket.on('schemaImport:tables', (preset, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const jdbcInfo = {user: preset.user, password: preset.password};

                            return agent.metadataTables(preset.jdbcDriverJar, preset.jdbcDriverClass, preset.jdbcUrl, jdbcInfo,
                                preset.schemas, preset.tablesOnly);
                        })
                        .then((tables) => cb(null, tables))
                        .catch((errMsg) => cb(errMsg));
                });

                // Return topology command result from grid to browser.
                socket.on('node:topology', (demo, attr, mtr, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.topology(demo, attr, mtr))
                        .then((clusters) => cb(null, clusters))
                        .catch((err) => cb(err));
                });

                // Close query on node.
                socket.on('node:query:close', (args, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            const cache = agent.ignite(args.demo).cache(args.cacheName);

                            return cache.__createPromise(cache._createCommand('qrycls').addParam('qryId', args.queryId));
                        })
                        .then(() => cb())
                        .catch((err) => cb(err));
                });

                // Execute query on node and return first page to browser.
                socket.on('node:query', (args, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            if (args.type === 'SCAN')
                                return agent.scan(args.demo, args.cacheName, args.pageSize);

                            return agent.fieldsQuery(args.demo, args.cacheName, args.query, args.pageSize);
                        })
                        .then((res) => cb(null, res))
                        .catch((err) => cb(err));
                });

                // Fetch next page for query and return result to browser.
                socket.on('node:query:fetch', (args, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.queryFetch(args.demo, args.queryId, args.pageSize))
                        .then((res) => cb(null, res))
                        .catch((err) => cb(err));
                });

                // Execute query on node and return full result to browser.
                socket.on('node:query:getAll', (args, cb) => {
                    // Set page size for query.
                    const pageSize = 1024;

                    agentMgr.findAgent(user._id)
                        .then((agent) => {
                            if (args.type === 'SCAN')
                                return agent.scan(args.demo, args.cacheName, pageSize);

                            return agent.fieldsQuery(args.demo, args.cacheName, args.query, pageSize);
                        })
                        .then((res) => {
                            const fetchResult = (fullRes) => {
                                if (fullRes.last)
                                    return fullRes;

                                return agent.queryFetch(args.demo, args.queryId, pageSize)
                                    .then((res) => {
                                        fullRes.rows = fullRes.rows.concat(res.rows);

                                        fullRes.last = res.last;

                                        return fetchResult(fullRes);
                                    })
                            };

                            return fetchResult(res);
                        })
                        .then((res) => cb(null, res))
                        .catch((errMsg) => cb(errMsg));
                });

                // Return cache metadata from all nodes in grid.
                socket.on('node:cache:metadata', (args, cb) => {
                    agentMgr.findAgent(user._id)
                        .then((agent) => agent.ignite(args.demo).cache(args.cacheName).metadata())
                        .then((caches) => {
                            let types = [];

                            const _compact = (className) => {
                                return className.replace('java.lang.', '').replace('java.util.', '').replace('java.sql.', '');
                            };

                            const _typeMapper = (meta, typeName) => {
                                let fields = meta.fields[typeName];

                                let columns = [];

                                for (const fieldName in fields) {
                                    if (fields.hasOwnProperty(fieldName)) {
                                        const fieldClass = _compact(fields[fieldName]);

                                        columns.push({
                                            type: 'field',
                                            name: fieldName,
                                            clazz: fieldClass,
                                            system: fieldName === '_KEY' || fieldName === '_VAL',
                                            cacheName: meta.cacheName,
                                            typeName
                                        });
                                    }
                                }

                                const indexes = [];

                                for (const index of meta.indexes[typeName]) {
                                    fields = [];

                                    for (const field of index.fields) {
                                        fields.push({
                                            type: 'index-field',
                                            name: field,
                                            order: index.descendings.indexOf(field) < 0,
                                            unique: index.unique,
                                            cacheName: meta.cacheName,
                                            typeName
                                        });
                                    }

                                    if (fields.length > 0) {
                                        indexes.push({
                                            type: 'index',
                                            name: index.name,
                                            children: fields,
                                            cacheName: meta.cacheName,
                                            typeName
                                        });
                                    }
                                }

                                columns = _.sortBy(columns, 'name');

                                if (!_.isEmpty(indexes)) {
                                    columns = columns.concat({
                                        type: 'indexes',
                                        name: 'Indexes',
                                        cacheName: meta.cacheName,
                                        typeName,
                                        children: indexes
                                    });
                                }

                                return {
                                    type: 'type',
                                    cacheName: meta.cacheName || '',
                                    typeName,
                                    children: columns
                                };
                            };

                            for (const meta of caches) {
                                const cacheTypes = meta.types.map(_typeMapper.bind(null, meta));

                                if (!_.isEmpty(cacheTypes))
                                    types = types.concat(cacheTypes);
                            }

                            return cb(null, types);
                        })
                        .catch((errMsg) => cb(errMsg));
                });

                const count = agentMgr.addAgentListener(user._id, socket);

                socket.emit('agent:count', {count});
            });

            // Handle browser disconnect event.
            io.sockets.on('disconnect', (socket) =>
                agentMgr.removeAgentListener(socket.client.request.user._id, socket)
            );
        }
    };
};
