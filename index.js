'use strict';
var fs = require('fs');
var Database = require('better-sqlite3');
var db = new Database('charging-stations.db');
var moment = require('moment-timezone');
const uuidV4 = require('uuid/v4');

function readFiles(dirname, onCheckReadFile, onError) {
    console.log("Read folder: " + dirname);
    fs.readdir(dirname, function(err, filenames) {
        if (err) {
            onError(err);
            return;
        }

        filenames.sort().forEach(function(filename) {

            onCheckReadFile(filename, function(err, read) {
                var fileDate = filename.substring(4, 14) + " " + filename.substring(15, 20)
                var fileDateLocal = moment.tz(fileDate, "America/New_York").tz("Europe/Berlin");
                if(read) {
                    var json = JSON.parse(fs.readFileSync(dirname + filename, 'utf8'));
                    var noLocations = 0;
                    var noNewTransactions = 0;
                    
                    json.forEach(function(location) {
                        var CLocation = location.name;
                        var DateTime = fileDateLocal.format();
                        var Timestamp = fileDateLocal.unix();
                        var ZipCode = location.plz;
                        var City = location.ort;
                        var Street = location.strasse;

                        if(location.acLinks) {
                            writeLocations(location.acLinks, 0);
                        }

                        if(location.acRechts) {
                            writeLocations(location.acRechts, 0);
                        }

                        if(location.ac) {
                            writeLocations(location.ac, 0);
                        }

                        if(location.dc) {
                            writeLocations(location.dc, 1);
                        }

                        function writeLocations(locations, dc) {
                            locations.forEach(function(singleLocation) {
                                noLocations++;
                                var Charger = singleLocation.bezeichnung;
                                var DC = dc;
                                var InUse = (singleLocation.status === "belegt") ? 1 : 0;
                                var TX = null;
                                var TXStart = 0;

                                if(InUse) {
                                    var transactionRow = 
                                        db.prepare('SELECT TX, InUse FROM POIData WHERE Charger=? AND Timestamp>=? AND Timestamp<? ORDER BY Timestamp DESC LIMIT 1')
                                            .get(Charger, Timestamp-3660000, Timestamp); // 61 Minutes
                                    if(transactionRow && transactionRow.InUse === 1 && transactionRow.TX) {
                                        TX = transactionRow.TX;
                                    } else {
                                        TX = uuidV4();
                                        TXStart = 1;
                                        noNewTransactions++;
                                    }     
                                }

                                db.prepare('INSERT INTO POIData(Location, Timestamp, DateTime, ZipCode, City, Street, Charger, DC, InUse, TX, TXStart) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)').
                                    run(CLocation, Timestamp, DateTime, ZipCode, City, Street, Charger, DC, InUse, TX, TXStart);
                            });
                        }
                    });

                    console.log("Read file: " + filename + " (" + fileDateLocal.format() + ") with " 
                    + noLocations + " Loc and " + noNewTransactions + " new Tx");

                } else {
                    console.log("Skip file: " + filename + " (" + fileDateLocal.format() + ")");
                }
            })
        });
    });
}

readFiles('poi/', 
    function(filename, callback) {
        var row = db.prepare('SELECT Count(FileName) AS C FROM FilesRead WHERE FileName=?').get(filename);
        if(row.C > 0) {
            callback(null, false);
        } else {
            db.prepare('INSERT INTO FilesRead(FileName) VALUES(?)').run(filename);
            callback(null, true);
        }
    },
    function(err) {
        throw err;
    }
);