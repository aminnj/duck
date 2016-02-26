// initializations
$(function() {
  init();

});


var maxElems = 5;
function addToTicker(text, delay) {
    var nElems = $("#ticker > span").length;
    if(nElems > maxElems) {
        // console.log(nElems);
        for(var i = nElems; i > maxElems; i--) { 
            if(nElems == maxElems + 1) $("#ticker").find(":first-child").slideUp('fast',function(){ $(this).remove(); }) ;
            else $("#ticker").find(":first-child").remove();
        }
    }
    // console.log("here: "+text + " " + nElems);
    if(delay) {
        $("<span style='display:block'>"+text+"</span>").hide().appendTo("#ticker").fadeIn(200);
    } else {
        $("<span style='display:block'>"+text+"</span>").appendTo("#ticker");
    }
}


var previous = "";
setInterval(function() {
    var ajax = new XMLHttpRequest();
    ajax.onreadystatechange = function() {
        if (ajax.readyState == 4) {
            if (ajax.responseText != previous) {
                nPrevious = previous.split("\n").length;
                newText = ajax.responseText;
                previous = ajax.responseText;
                newLines = newText.split("\n");
                if(newLines.length < nPrevious) return;

                newLines = newLines.slice(nPrevious-1);
                // remove empty strings from array
                newLines = newLines.filter(Boolean);
                
                if(previous != "") {
                    var anim = newLines.length == 1;
                    for(var iline = 0; iline < newLines.length; iline++) {


                        // console.log(newLines[iline]);
                        addToTicker( newLines[iline], iline == newLines.length-1 );

                        // $("#ticker > ul").find(":first-child").remove();
                        // $("#ticker > ul").append("<li class='tickerItem'>" + newLines[iline] + "</li>");
                        // $("#ticker").vTicker('next', {animate: iline==newLines.length-1});
                    }
                }

            }
        }
    };
    ajax.open("POST", "test.txt", true); //Use POST to avoid caching
    ajax.send();
}, 100000);

var detailsVisible = false;

function init() {
    // $.getJSON("http://uaf-6.t2.ucsd.edu/~namin/dump/test.json", function(data) { parseJson(data); });
    // WOW CAN PUT EXTERNAL URLS HERE MAN!
    $.getJSON("data_old.json", function(data) { parseJson(data); });
}


function getDetails(sample) {
    var stat = sample["status"];

    var buff = "";

    if(stat == "crab") {
        var crab = sample["crab"];
        var breakdown = crab["breakdown"];
        buff += "<br><span class='bad'>failed: " + breakdown["failed"] + "</span>";
        buff += "<br>cooloff: " + breakdown["cooloff"];
        buff += "<br>idle: " + breakdown["idle"];
        buff += "<br>unsubmitted: " + breakdown["unsubmitted"];
        buff += "<br>running: " + breakdown["running"];
        buff += "<br>transferring: " + breakdown["transferring"];
        buff += "<br>transferred: " + breakdown["transferred"];
        buff += "<br><span class='good'>finished: " + breakdown["finished"] + "</span>";
    }
    return buff;
}

function getProgress(sample) {
    var stat = sample["status"];
    var done = 0;
    var tot = 1;

    if (stat == "new") return 0.0;
    else if (stat == "crab") {

        if("breakdown" in sample["crab"]) {
            done = sample["crab"]["breakdown"]["finished"];
            tot = sample["crab"]["njobs"];
        }
        return 5.0 + 40.0*(done/tot);

    } else if (stat == "postprocessing") {

        if("postprocessing" in sample) {
            done = sample["postprocessing"]["done"];
            tot = sample["postprocessing"]["total"];
        }
        return 55.0 + 35.0*(done/tot);

    } else if (stat == "done") return 100.0;
    else return -1.0;

}

function syntaxHighlight(json) {
    // stolen from http://stackoverflow.com/questions/4810841/how-can-i-pretty-print-json-using-javascript
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(?:\.\d*)?(?:[eE][+\-]?\d+)?)/g, function (match) {
        var cls = 'number';
        if (/^"/.test(match)) {
            if (/:$/.test(match)) {
                cls = 'key';
            } else {
                cls = 'string';
            }
        } else if (/true|false/.test(match)) {
            cls = 'boolean';
        } else if (/null/.test(match)) {
            cls = 'null';
        }
        return '<span class="' + cls + '">' + match + '</span>';
    });
}


function parseJson(data) {

    var date = new Date(data["last_updated"]*1000); // ms to s
    var hours = date.getHours();
    var minutes = "0" + date.getMinutes();
    var seconds = "0" + date.getSeconds();
    var formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
    $("#last_updated").text("Last updated at " + date.toLocaleTimeString() + " on " + date.toLocaleDateString());

    for(var i = 0; i < data["samples"].length; i++) {
        var sample = data["samples"][i];
        var container = $("#section_1");
        container.append("<br>");
        container.append("<a href='#/' class='thick' onClick=\"$('#details_"+i+"').slideToggle(150)\">"+sample["dataset"]+"</a>");

        container.append("<div class='pbar' id='pbar_"+i+"'><span id='pbartext_"+i+"' class='pbartext'></span></div>");

        // FIXME, display:none
        container.append("<div id='details_"+i+"' style='display:none;'></div>");
        // container.append("<div id='details_"+i+"' class='details' ></div>");

        $( "#pbar_"+i ).progressbar({max: 100});

        var pct = Math.round(getProgress(sample));

        var color = 'hsl(' + pct*1.2 + ', 70%, 50%)';
        $("#pbar_"+i).progressbar("option","value",pct);
        $("#pbar_"+i).find(".ui-progressbar-value").css({"background": color});
        $("#pbartext_"+i).text(sample["status"] + " [" + pct + "%]");

        var jsStr = syntaxHighlight(JSON.stringify(sample, undefined, 4));

        // turn crab into a link to the dashboard
        if(("crab" in sample) && ("uniquerequestname" in sample["crab"])) {
            var urn = sample["crab"]["uniquerequestname"];
            var link = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=default&refresh=0&table=Jobs&status=&site=&tid="+urn;
            jsStr = jsStr.replace("\"crab\":", " <a href='"+link+"' style='text-decoration: underline'>crab</a>: ");
        }
        
        // bold the output directory if it's done
        if(("finaldir" in sample) && (sample["status"] == "done")) {
            jsStr = jsStr.replace("\"finaldir\":</span> <span class=\"string\">", "\"finaldir\":</span> <span class=\"boldString\">");
        }

        // turn dataset into a link to DAS
        jsStr = jsStr.replace("\"dataset\":", " <a href='https://cmsweb.cern.ch/das/request?view=list&limit=50&instance=prod%2Fglobal&input="+sample["dataset"]+"' style='text-decoration: underline'>dataset</a>: ");
        $("#details_"+i).append("<pre>" + jsStr + "</pre>");


    }
}

function expandAll() {
    // do it this way because one guy may be reversed
    if(detailsVisible) {
        $("#toggle_all").text("show details")
        $("[id^=details_]").slideUp(150);
    } else {
        $("#toggle_all").text("hide details")
        $("[id^=details_]").slideDown(150);
    }
    detailsVisible = !detailsVisible;
}
