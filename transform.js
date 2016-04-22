var csv = require("fast-csv"), 
    fs = require("fs"), 
    path = require("path"), 
    commandLineArgs = require('command-line-args'),
    moment = require('moment');
    getUsage = require("command-line-usage");

const cliDefinitons = [
    { name: 'help', alias: 'h', type: Boolean, description: 'Display this message.' },
    { name: 'verbose', alias: 'v', type: Boolean, description: 'Enable verbose Logging' },
    { name: 'debug', type: Boolean, description: 'Enable debug output in CSV' },
    { name: 'files', type: String, multiple: true, defaultOption: true, description: "Can also be provided as the last two arguments without a parameter name. The parameter takes up to two arguments: 1st argument is interpreted as source CSV file path. The 2nd one is optinal an is interpreted as destination CSV file. If no second file path is given the destination file wil have a '_transformed' added to the original filename."},
    { name: 'dateFormat', alias: 'f', type: String, defaultValue: 'DD.MM.YYYY', description: "Default: 'DD.MM.YYYY'. For details see http://momentjs.com/docs/#/parsing/string-format/"},
    { name: 'dateColumn', type: String, defaultValue: 'Datum', description: "The name of the column in the source file that holds the date. Default: 'Datum'"},
    { name: 'timeFormat', alias: 't', type: String, defaultValue: 'hh:mm', description: "Default: 'hh:mm'. For details see http://momentjs.com/docs/#/parsing/string-format/"},
    { name: 'timeColumn', type: String, defaultValue: 'Uhrzeit', description: "The name of the column in the source file that holds the time. Default: 'Uhrzeit'"},
    { name: 'sourcePowerColumn', alias: 'p', type: String, defaultValue: 'Wert in KW', description: "The name of the column in the source file that holds the power value. Default: 'Wert in KW'"},
    { name: 'destPowerColumn', alias: 'o', type: String, defaultValue: 'PowerConsumed', description: "The name of the column in the destination file that will hold the power value. Default: 'PowerConsumed'"}, //TODO make avg interpretation configigurable
    { name: 'copyPowerColumns', type: String, multiple: true, description: "An optinal list of column names that will have copies of the powe value."},
    { name: 'calculateWork', alias: 'w', type: Boolean, description: "If given, work values will be calculated for each power value as counter starting at 0."}, //TODO make configigurable
    { name: 'isUTCTimestamp', alias: 'u', type: Boolean, description: "If given, the source file is expected to have a column 'Timestamp' that holds a UTC timstamp. All date and time format options will be ignored."},
    { name: 'powerMultiplier', alias: 'm', type: Number, defaultValue: 1000, description: "The number that the source value needs to be multiplied with to get W. So if the the source is in kW the multiplier needs to be 1000. Default: 1000"},
];

var cli = commandLineArgs(cliDefinitons);

const getUsageOptions = {
  title: 'power-transformer',
  description: 'Takes a CSV file an transforms average power values in the given csv and transforms them in a way that they represent average values.',
  footer: 'Project home: [underline]{https://github.com/soeren-lubitz/power-transformer}'
}

var options = cli.parse();

var CircularBuffer = require("circular-buffer");
var buf = new CircularBuffer(10);

var vlog = function() {
  options.verbose && console.log(Array.prototype.slice.call(arguments));
};

vlog(options);

if (!options.files || !options.files[0]) {
    console.error("Please provide source file!");
    console.log(getUsage(cliDefinitons, getUsageOptions));
    process.exit(1);
}

var sourceFileName = options.files[0];
var destFielName =  path.join(
    path.dirname(options.files[0]), 
    path.basename(options.files[0], 
    path.extname(options.files[0])) + '_transformed' + path.extname(options.files[0])
);

if (options.files[1]) {
    destFielName = options.files[1];
}

var csvStream = csv.createWriteStream({headers: true, delimiter: ";", quote:null});
var writableStream = fs.createWriteStream(destFielName);

writableStream.on("finish", function(){
  console.log("DONE!");
});

csvStream.pipe(writableStream);


var lastRow = null;
var work = 0;
var granularity = null;
var firstRun = true;

var stream = fs.createReadStream(sourceFileName)
    .pipe(csv.parse({headers: true, delimiter: ";"}))
    .transform(function (row) {
        var rowObject = {
            timestamp: null,
            value: null
        };

        if (options.isUTCTimestamp) {
            rowObject.timestamp = parseInt(row.Timestamp);
        } else {
            var dateTimeString = row[options.dateColumn] + ' ' +  row[options.timeColumn];
            var dateFormat = options.dateFormat + ' ' + options.timeFormat;
            var dateTime = moment(dateTimeString, dateFormat);
            rowObject.timestamp = dateTime.unix();
            rowObject.date = dateTimeString;
            rowObject.shifted = dateTime.isDSTShifted();
            rowObject.moved = false;
            rowObject.isDST = dateTime.isDST();
        }

        fixDoubleHourDSTShift(rowObject);

        if (!rowObject.shifted && !rowObject.moved) {
            buf.enq(rowObject.timestamp);
        }

        var powerValue = row[options.sourcePowerColumn].replace(',','.');
        rowObject.value = parseFloat(powerValue) * options.powerMultiplier;


        return rowObject;
    })
    .on("readable", function () {


        while (null !== (row = stream.read())) {
            if (row.shifted) {
                continue;
            }

            if (firstRun) {
                if (options.debug) {
                    csvStream.write(getOutputRow(row.timestamp - 1, 'null', 'null', 'xxx', false, true));
                } else {
                   csvStream.write(getOutputRow(row.timestamp - 1, 'null', 'null')); 
                }
                firstRun = false;
            }

            
            if (lastRow !== null) {
                csvStream.write(getOutputRow(row.timestamp-1, lastRow.value, work));
                granularity = row.timestamp - lastRow.timestamp;
                work = work + powerToWork(lastRow.value, granularity);
            }
            if (options.debug) {
                csvStream.write(getOutputRow(row.timestamp, row.value, work, row.date, row.shifted, row.isDST));
            } else {
               csvStream.write(getOutputRow(row.timestamp, row.value, work)); 
            }

            lastRow = row;
        }
    })
    .on("end", function() {
        csvStream.write(getOutputRow(lastRow.timestamp + 1, 'null', 'null'));
        csvStream.end();
        process.exit;
    });

var powerToWork = function(power, granularity) {
    if (!granularity) {
        return 0;    
    }
    return power / (3600 / granularity);
};

var getOutputRow = function(timestamp, power, work, date, shifted, dst) {
    var outputRow = {Timestamp: timestamp};

    outputRow[options.destPowerColumn] = power;

    if (options.calculateWork) {
        outputRow['Work' + options.destPowerColumn.replace('Power','')] = work;
    }

    if (options.copyPowerColumns) {
        for (var i = 0; i < options.copyPowerColumns.length; i++) {
            var powerCol = options.copyPowerColumns[i];

            outputRow[powerCol] = power;

            if (options.calculateWork) {
                outputRow['Work' + powerCol.replace('Power','')] = work;
            }
        }
    }

    if (date) {
        outputRow.SourceDate = date;
        outputRow.isDSTShifted = shifted;
        outputRow.isDST = dst;
    }

    return outputRow;


};

var fixDoubleHourDSTShift = function(rowObject) {
    var knownTimestamps = buf.toarray();
    
    if (knownTimestamps.indexOf(rowObject.timestamp) > -1) {
        vlog('DoubleHourDSTShift for Date detected.', rowObject.date,' Shifting', rowObject.timestamp, 'to', rowObject.timestamp + 3600);
        rowObject.timestamp = rowObject.timestamp + 3600;
        rowObject.moved = true;
    }

}