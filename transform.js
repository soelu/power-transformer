var csv = require("fast-csv"), fs = require("fs"), 
    path = require("path");

var csvStream = csv.createWriteStream({headers: true, delimiter: ";", quote:null});
var writableStream = fs.createWriteStream(process.argv[3]);

writableStream.on("finish", function(){
  console.log("DONE!");
});

csvStream.pipe(writableStream);

var lastRow = null;
var work = 0;
var granularity = null;
var firstRun = true;

var tag = process.argv[4] || 'PowerConsumed';

var stream = fs.createReadStream(process.argv[2])
    .pipe(csv.parse({headers: true, delimiter: ";"}))
    .transform(function (row) {
        return {
            timestamp: parseInt(row.Timestamp),
            value: parseFloat(row[tag])
        };
    })
    .on("readable", function () {

        while (null !== (row = stream.read())) {
            if (firstRun) {
                csvStream.write({
                    Timestamp: row.timestamp - 1, 
                    PowerIn: 'null',
                    PowerConsumed: 'null',
                    WorkIn: 'null',
                    WorkConsumed: 'null'
                });
                firstRun = false;
            }

            
            if (lastRow !== null) {
                csvStream.write({
                    Timestamp: row.timestamp-1, 
                    PowerIn: lastRow.value,
                    PowerConsumed: lastRow.value, 
                    WorkIn: work,
                    WorkConsumed: work
                });
                granularity = row.timestamp - lastRow.timestamp;
                work = work + powerToWork(lastRow.value, granularity);
            }
            csvStream.write({
                Timestamp: row.timestamp, 
                PowerIn: row.value,
                PowerConsumed: row.value,
                WorkIn: work,
                WorkConsumed: work
            });

            lastRow = row;
        }
    })
    .on("end", function() {
        csvStream.write({
                Timestamp: lastRow.timestamp + 1, 
                PowerIn: 'null',
                PowerConsumed: 'null',
                WorkIn: 'null',
                WorkConsumed: 'null'
            });
        csvStream.end();
        process.exit;
    });

var powerToWork = function(power, granularity) {
    if (!granularity) {
        return 0;    
    }
    return power / (3600 / granularity);
};