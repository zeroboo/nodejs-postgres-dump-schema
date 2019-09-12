#!/usr/bin/env node
const prog = require('caporal');
const util = require('util');

prog.version('1.0.0')
    // you specify arguments using .argument()
    // 'app' is required, 'env' is optional
    .description('A Postgres schema exporter')
    .argument('<config_file>', 'Configurations for connecting to Postgres', '')
    .argument('[output_folder]', 'Output folder', '', 'output')
    // you specify options using .option()
    // if --tail is passed, its value is required
    .action(function (args, options, logger) {
        // args and options are objects
        // args = {"app": "myapp", "env": "production"}
        // options = {"tail" : 100}
        logger.info("Exporting: ");
        logger.info("Loading config from file:", args.configFile);

        const fs = require('fs');

        var rimraf = require("rimraf");
        rimraf(args.outputFolder, function () {
            console.log("done");
            var functionFolder = args.outputFolder + "/Functions";
            var tableFolder = args.outputFolder + "/Tables";

            fs.mkdirSync(functionFolder, { recursive: true });
            fs.mkdirSync(tableFolder, { recursive: true });

            let rawdata = fs.readFileSync(args.configFile);
            let config = JSON.parse(rawdata);

            const { Client } = require('pg');
            const client = new Client(config)
            client.connect().then(function () {
                return client.query(util.format(
                    "SELECT DISTINCT(proname)"
                    + " FROM pg_catalog.pg_proc f"
                    + " INNER JOIN pg_catalog.pg_namespace n on (f.pronamespace = n.oid)"
                    + " INNER JOIN pg_catalog.pg_user u on f.proowner = u.usesysid "
                    + " WHERE n.nspname='%s'"
                    , config.schema));
            })
                .then(function (result) {
                    let loadSourcePromises = [];
                    logger.debug("Found", result.rows.length, "function names");
                    result.rows.forEach(function (f) {
                        logger.info("Extracting function", f.proname);
                        let sqlSrc = util.format(
                            "SELECT proname, pronargs, f.prosrc"
                            + " FROM pg_catalog.pg_proc f"
                            + " INNER JOIN pg_catalog.pg_namespace n on (f.pronamespace = n.oid)"
                            + " INNER JOIN pg_catalog.pg_user u on f.proowner = u.usesysid "
                            + " WHERE n.nspname='%s' AND proname='%s'"
                            , config.schema, f.proname);

                        let promise = client.query(sqlSrc).then(function (srcResult) {
                            logger.info("Found", srcResult.rows.length, "function name", f.proname);
                            srcResult.rows.forEach(function (srcRecord) {
                                let fileName = functionFolder + "/" + srcRecord.proname + ".sql";
                                let counter = 0;
                                while (fs.existsSync(fileName)) {
                                    counter++;
                                    fileName = functionFolder + "/" + srcRecord.proname + "_" + counter + ".sql";
                                }
                                logger.debug("Extracting to file", fileName);
                                fs.writeFileSync(fileName, srcRecord.prosrc);
                            });
                        }).catch(function (error) {
                            console.log("Get src error: ", error, sqlSrc);
                        });
                        loadSourcePromises.push(promise);
                    });
                    return Promise.all(loadSourcePromises);
                })
                .then(function (src) {
                    console.log("Export functions DONE!");
                    let sqlGetTableName = util.format("SELECT * FROM information_schema.tables WHERE table_catalog='%s' AND table_schema='%s' AND table_type='BASE TABLE'"
                        , config.database, config.schema);
                    return client.query(sqlGetTableName)
                        .then(function (result) {
                            let promises = [];
                            result.rows.forEach(function(row){
                                logger.debug("Found table: "row.table_name);
                            });
                            return Promise.all(promises);
                        })
                        .catch(function(error){
                            logger.error("Error: ", sqlSrc, error);
                        });
                })
                .catch(function (error) {
                    logger.error("Error: " + error.message);
                    console.log(error)
                })
                .finally(function () {
                    client.end();
                });
        });


    });

prog.parse(process.argv);

// ./myprog deploy myapp production --tail 100
console.log("Helloworld");