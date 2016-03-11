var alldata = {};
var jsonFile = "data.json";
var baseDir = "BASEDIR_PLACEHOLDER";
var refreshSecs = 10*60;
var detailsVisible = false;
var duckMode = false;
var adminMode = false;

$(function() {
    loadJSON();
    setInterval(loadJSON, refreshSecs*1000);

    handleDuckMode();
    handleOtherTwiki();
    handleSubmitButton();
    handleAdminMode();

});

$.ajaxSetup({
   type: 'POST',
   timeout: 5000,
});

function handleSubmitButton() {
    $('.submitButton').click(function (e) {
        if (e.target) {
            if(e.target.value == "fetch" || e.target.value == "update") {
                doTwiki(e.target.value);
            } else if(e.target.value == "addinstructions") {
                // console.log("adding to instructions");
                addInstructions(e.target.value);
            }
        }
    });
}

function handleOtherTwiki() {
    $( "#selectPage" ).change(function() {
        if($(this).find(":selected").text()=="Other") {
            $("#otherPage").show();
        } else {
            $("#otherPage").hide();
        }
    });
}

function handleAdminMode() {
    $( "#firstTitle" ).click(function() { 
        $( "#admin" ).slideToggle(150); 
        if(adminMode) {
            adminMode = false;
            fillDOM(alldata);
        } else {
            adminMode = true;
            fillDOM(alldata);
        }
    });
}

function handleDuckMode() {
    $( ".mainlogo" ).on('contextmenu dblclick',function() { 
        if(duckMode) {
            duckMode = false;
            $(".mainlogo").attr('src', 'images/crab.png');
            $("#container").css("background", "");
            $("#firstTitle").text("auto");
            $(".duckAudio").trigger('pause');
        } else {
            duckMode = true;
            $(".mainlogo").attr('src', 'images/ducklogo.png');
            $("#container").css("background", "url(images/ducklogo.png");
            $("#firstTitle").text("duck");
            $(".duckAudio").prop("currentTime",0);
            $(".duckAudio").trigger('play');
        }
    });
}

function loadJSON() {
    // $.getJSON("http://uaf-6.t2.ucsd.edu/~namin/dump/test.json", function(data) { parseJson(data); });
    // WOW CAN PUT EXTERNAL URLS HERE MAN!
    $.getJSON(jsonFile, function(data) { 
        if(!("samples" in alldata) || (data["samples"].length != alldata["samples"].length)) {
            setUpDOM(data);
        }
        fillDOM(data); 
    });
}

function doTwiki(type) {
    $("#twikiTextarea").val("Loading...");
    $("#message").html("");

    var formObj = {};
    formObj["action"] = type;
    if(type == "update") {
        var donesamples = [];
        for(var i = 0; i < alldata["samples"].length; i++) {
            donesamples.push( alldata["samples"][i] );
        }
        console.log(donesamples);
        formObj["samples"] = JSON.stringify(donesamples);
    }
    var inputs = $("#fetchTwikiForm").serializeArray();
    $.each(inputs, function (i, input) {
        formObj[input.name] = input.value;
    });
    console.log(formObj);
    $.ajax({
            url: "./handler.py",
            type: "POST",
            data: formObj,
            success: function(data) {
                    console.log(data);
                    $("#twikiTextarea").val(data);
                },
            error: function(data) {
                    $("#message").html("<span style='color:red'>Error:</span> "+data["responseText"]);
                    console.log(data);
                },
       });
}

function addInstructions(type) {
    var formObj = {};
    formObj["action"] = type;
    formObj["data"] = $("#twikiTextarea").val();
    formObj["basedir"] = baseDir;
    console.log(formObj);
    $.ajax({
            url: "./handler.py",
            type: "POST",
            data: formObj,
            success: function(data) {
                    $("#message").html("<span style='color:green'>"+data+"</span>");
                    console.log(data);
                },
            error: function(data) {
                    $("#message").html("<span style='color:red'>Error:</span> "+data["responseText"]);
                    console.log(data);
                },
       });
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
            if(tot < 1) tot = 1;
        }
        return 1.0 + 65.0*(done/tot);

    } else if (stat == "postprocessing") {

        if("postprocessing" in sample) {
            done = sample["postprocessing"]["done"];
            tot = sample["postprocessing"]["total"];
        }
        return 68.0 + 30.0*(done/tot);

    } else if (stat == "done") return 100;
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

function setUpDOM(data) {
    var container = $("#section_1");
    container.empty(); // clear the section
    for(var i = 0; i < data["samples"].length; i++) {
        var sample = data["samples"][i];
        var toappend = "";
        toappend += "<br>";
        toappend += "<a href='#/' class='thick' onClick=\"$('#details_"+i+"').slideToggle(100)\">"+sample["dataset"]+"</a>";
        toappend += "<div class='pbar' id='pbar_"+i+"'>";
        toappend +=      "<span id='pbartextleft_"+i+"' class='pbartextleft'></span>";
        toappend +=      "<span id='pbartextright_"+i+"' class='pbartextright'></span>";
        toappend += "</div>";
        toappend += "<div id='details_"+i+"' style='display:none;'></div>";

        container.append(toappend);

        $( "#pbar_"+i ).progressbar({max: 100});
        $("#pbar_"+i).progressbar("option","value",0);
    }
}

function fillDOM(data) {
    alldata = data;

    var date = new Date(data["last_updated"]*1000); // ms to s
    var hours = date.getHours();
    var minutes = "0" + date.getMinutes();
    var seconds = "0" + date.getSeconds();
    var formattedTime = hours + ':' + minutes.substr(-2) + ':' + seconds.substr(-2);
    $("#last_updated").text("Last updated at " + date.toLocaleTimeString() + " on " + date.toLocaleDateString());

    for(var i = 0; i < data["samples"].length; i++) {
        var sample = data["samples"][i];

        var pct = Math.round(getProgress(sample));
        var color = 'hsl(' + pct*0.8 + ', 70%, 50%)';
        if(pct == 100) {
            // different color if completely done
            color = 'hsl(' + pct*1.2 + ', 70%, 50%)';
        }

        $("#pbar_"+i).progressbar("value", pct);
        $("#pbar_"+i).find(".ui-progressbar-value").css({"background": color});
        $("#pbartextright_"+i).html(sample["status"] + " [" + pct + "%]");
        $("#pbartextleft_"+i).html(""); 

        if(adminMode) {
            $("#pbartextleft_"+i).html("<a href='#/' onClick='console.log($(this).parent().parent().prev().text());'>&#9762;</a>"); 
        }

        var jsStr = syntaxHighlight(JSON.stringify(sample, undefined, 4));

        // turn crab into a link to the dashboard
        if(("crab" in sample) && ("uniquerequestname" in sample["crab"])) {
            var urn = sample["crab"]["uniquerequestname"];
            var link = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#user=default&refresh=0&table=Jobs&status=&site=&tid="+urn;
            jsStr = jsStr.replace("\"crab\":", " <a href='"+link+"' style='text-decoration: underline'>crab</a>: ");
        }
        
        // bold the output directory and event counts if it's done
        if(("finaldir" in sample) && (sample["status"] == "done")) {
            jsStr = jsStr.replace("\"finaldir\":</span> <span class=\"string\">", "\"finaldir\":</span> <span class=\"string bold\">");
            jsStr = jsStr.replace("\"nevents_DAS\":</span> <span class=\"number\">", "\"nevents_DAS\":</span> <span class=\"number bold\">");
            jsStr = jsStr.replace("\"nevents_unmerged\":</span> <span class=\"number\">", "\"nevents_unmerged\":</span> <span class=\"number bold\">");
            jsStr = jsStr.replace("\"nevents_merged\":</span> <span class=\"number\">", "\"nevents_merged\":</span> <span class=\"number bold\">");
        }

        // turn dataset into a link to DAS
        jsStr = jsStr.replace("\"dataset\":", " <a href='https://cmsweb.cern.ch/das/request?view=list&limit=50&instance=prod%2Fglobal&input="+sample["dataset"]+"' style='text-decoration: underline'>dataset</a>: ");
        $("#details_"+i).html("<pre>" + jsStr + "</pre>");
    }

}

function expandAll() {
    // do it this way because one guy may be reversed
    if(detailsVisible) {
        $("#toggle_all").text("show details")
        $("[id^=details_]").slideUp(100);
    } else {
        $("#toggle_all").text("hide details")
        $("[id^=details_]").slideDown(100);
    }
    detailsVisible = !detailsVisible;
}
