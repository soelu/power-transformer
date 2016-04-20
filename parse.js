var csv = require("fast-csv"), fs = require("fs"), 
    path = require("path");

var csvStream = csv.createWriteStream({headers: true, delimiter: ";"});
var writableStream = fs.createWriteStream(process.argv[3]);

writableStream.on("finish", function(){
  console.log("DONE!");
});

csvStream.pipe(writableStream);

var lastRow = null;

var stream = fs.createReadStream(process.argv[2])
    .pipe(csv.parse({headers: true, delimiter: ";"}))
    .transform(function (row) {
        return {
            timestamp: parseInt(row.Timestamp),
            value: parseFloat(row.PowerIn)
        };
    })
    .on("readable", function () {
        while (null !== (row = stream.read())) {
            if (lastRow != null) {
                csvStream.write({Timestamp: row.timestamp-1, PowerIn: lastRow.value});
            }
            csvStream.write({Timestamp: row.timestamp, PowerIn: row.value});
            lastRow = row;
        }
    })
    .on("end", function() {
        csvStream.end();
        process.exit;
    });