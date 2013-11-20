/* news_data : https://github.com/lbracken/news_data
   :license: MIT, see LICENSE for more details.
*/
var URL_GET_TERM_COUNTS = "get_term_counts";

var graph = null;
var graphLines = null;

var barChart = null;
var barChartBars = null;

var hoveredListItem = null;
var hoveredBarChartFlag = null;
var hoveredGraphItemGlow = null;

var searchInProgress = false;



// ****************************************************************************
// *                                                                          *
// *  Terms AutoComplete                                                      *
// *                                                                          *
// ****************************************************************************

// Setup autocomplete, populate suggestion values and attached needed listeners
function setupAutocomplete(data) {	

	$("#addTermText").focus();
	$("#addTermText").keydown(function(event){
		if (event.keyCode == '13') {
			addTerm();
		}
	});
}

function disableAddTerm() {
	$("#addTermText").autocomplete({disabled: true});
	$("#addTermText").autocomplete("close");
	$("#addTermText").attr("disabled", "disabled");
	$("#addTermTextLoading").fadeIn();
}

function enableAddTerm() {
	$("#addTermTextLoading").hide();
	$("#addTermText").val("");
	$("#addTermText").autocomplete({disabled: false});
	$("#addTermText").removeAttr("disabled"); 
	$("#addTermText").focus();
}

//****************************************************************************
//*                                                                          *
//*  Manage Time Range                                                       *
//*                                                                          *
//****************************************************************************

function getTimeStart() {
	return 946702800;
}

function getTimeEnd() {
	//return 978238800;
	return 1357016399;
}

//****************************************************************************
//*                                                                          *
//*  Manage Application State                                                *
//*                                                                          *
//****************************************************************************

function getCurrentState() {

	var state = {};
	state.terms = [];
	
	// Parse key-value pairs from URL Hash
	var args = window.location.hash.substring(1).split('&');
	for (var i = 0; i < args.length; i++) {

		var pair = args[i].split('=');
		if (pair.length !== 2 || pair[1] === "") {
			continue;
		}

		var key = decodeURIComponent(pair[0])
		if (key === "terms") {
			state.terms = decodeURIComponent(pair[1]).split(',');
		} else if (key === "time_start") {
			// ...
		} else if (key === "time_end") {
			// ...
		}
	}

	return state;
}

function updateState(state) {

	hash = "";
	var i = 0;

	if(state && state.terms) {

		// Ensure there are no empty string terms
		while (i < state.terms.length) {
			if("" === $.trim(state.terms[i])) {
				state.terms.splice(i, 1);
			} else {
				i++;
			}
		}

		hash = "terms=" + state.terms.toString();
	}

	window.location.hash = hash;
}


function addTerm() {
	// Prevent multiple searches
	if (searchInProgress) {
		return; 
	}	  
	          
	// Get the term to add, and update the current state      
	var term = $("#addTermText").val();
	term = $.trim(term);

	if (term) {
		currState = getCurrentState();

		// Don't allow duplicates
		for (var i = 0; i < currState.terms.length; i++) {
			if (term === currState.terms[i]) {
				enableAddTerm();
				return;
			}	
		}

		currState.terms.push(term);
		updateState(currState);
	}
}

function onStateChange() {

	currState = getCurrentState();
	if (currState.terms && currState.terms.length > 0) {

		searchInProgress = true;    
		disableAddTerm();

		var requestData = getRequestData(currState);
		$.getJSON(URL_GET_TERM_COUNTS, requestData, onTimeSeriesTermCountResponse);
		// TODO: Handle failure case...
	} else {
		// TODO: Pick a better function than this...
		onTimeSeriesTermCountResponse();
	}
}

function removeTerm(term) {

	if (term) {
		currState = getCurrentState();
		for (var i = 0; i < currState.terms.length; i++) {
			if (term === currState.terms[i]) {
				currState.terms.splice(i, 1);
				break;
			}
		}

		updateState(currState);
	}
}

function onTimeSeriesTermCountResponse(data) {

	// Update Graph & Terms List
	updateGraph(data);
	updateTotalsBarChart(data);
	updateTermsList(data);
	updateDownloadLink();
	
	enableAddTerm();
	searchInProgress = false;
}


function getRequestData(currState) {

	var requestData = {
		terms : currState.terms.toString(),
		time_start: getTimeStart(),
		time_end: getTimeEnd()
	};

	return requestData;
}


//****************************************************************************
//*                                                                          *
//*  Time Series Graph Logic                                                 *
//*                                                                          *
//****************************************************************************

function updateGraph(data) {

	// TOOD: Animate graph while rendering?

	graph.clear();
	if (data && data.terms && data.terms.length > 0) {

		// Setup graph labels and text
		graph.text(75,10,"(mentions)");

		// Assemble data for the chart and determine the max value
		var yMax = 0;
		var valuesY = [];
		for (var i = 0; i < data.terms.length; i++) {
			console.log(' Length for [' + data.terms[i].term + '] : ' + data.terms[i].data.length);
			valuesY.push(data.terms[i].data);
			if (data.terms[i].max > yMax) {
				yMax = data.terms[i].max;
			}
		}

		var valuesX = [];
		for (var i = 0; i < valuesY[0].length; i++) {
			valuesX.push(i);
		};

		var xAxisStep = 6;
		var yAxisStep = (yMax > 5) ? 5 : 1;
		graphLines = graph.linechart(32, 0, 843, 325, valuesX, valuesY, {
			axis: "0 0 1 1",
			axisxstep: xAxisStep,
			axisystep: yAxisStep,
			smooth: true
			//shade: true, nostroke: true
		});

		// Setup each line
  		for (var i = 0; i < graphLines.lines.length; i++) {
  			graphLines.lines[i].id = "termLineItem_" + data.terms[i].term;
  		}

		// Modify the x-axis labels
		var xText = graphLines.axis[0].text.items;
		var dateIncrement = (data.time_end - data.time_start) / xAxisStep;

		// If granularity is DAILY, round up a day to ensure
		// that the x-axis labels are more meaningful.
		if (data.granularity === "DAILY") {
			dateIncrement += (60 * 60 * 24);
		}

		for (var i in xText){
			var date = new Date((data.time_start + (i * dateIncrement)) * 1000);
			xText[i].attr({'text': formatDateShort(date)});
  		};

  		// Modify the y-axis labels
  		var yText = graphLines.axis[1].text.items;
  		for (var i in yText) {
  			var formattedNumber = formatNumber(yText[i].attr("text"));
  			yText[i].attr({'text' : formattedNumber});
  		}
	}
}

//****************************************************************************
//*                                                                          *
//*  Totals Bar Chart Logic                                                  *
//*                                                                          *
//****************************************************************************

function updateTotalsBarChart(data) {

	// Clear the chart and see if there's data to render.  No need to show the
	// chart unless there are atleast two values to compare.
	barChart.clear();
	$("#termsTotal").hide();
	if (data && data.terms && data.terms.length > 1) {

		// Assemble data for the bar chart
		var values = [];
		for (var i = 0; i < data.terms.length; i++) {
			values.push(data.terms[i].total);
		}

		barChartBars = barChart.hbarchart(0,5, 250, 75, values, {"type" : "soft"});
		barChartBars.hover(onHoverInTerm, onHoverOutTerm);

		// Setup each bar
  		for (var i = 0; i < barChartBars.bars.length; i++) {
  			var term = data.terms[i];
  			barChartBars.bars[i].id = "termBarItem_" + term.term;
  			barChartBars.bars[i].title = term.term + " : " +  formatNumber(term.total) + " total mentions";
  		}

		$("#termsTotal").show();
	}
}

function updateTermsList(data) {

	// Clear the terms list
	$("#termsList").hide();
	$("#termsList").html("");

	if (data && data.terms && data.terms.length > 0) {

		// Iterate over the list of terms
		for (var i = 0; i < data.terms.length; i++) {
			renderTermsListItem(data.terms[i], i);
		}
	}

	$("#termsList").fadeIn("slow");
}

function renderTermsListItem(term, ctr) {

	var line = graphLines.lines[ctr];
	var termsListItem      = $("<div class='termsListItem'></div>");
	var termsListItemInner = $("<div class='termsListItem-Inner'></div>");
	var termsListItemClose = $("<div class='termsListItem-Close'></div>");
	var termsListItemCloseImg = $("<img src='static/images/icons/close.png' title='Remove from list'/>");

	termsListItem.append(termsListItemInner);
	termsListItem.append(termsListItemClose);
	termsListItemClose.append(termsListItemCloseImg);
		
	termsListItem.attr("id", "termListItem_" + term.term);	
	termsListItem.hover(onHoverInTerm, onHoverOutTerm);

	// Use the same color as the graph line
	termsListItem.css("background-color", Raphael.getRGB(line.attr("stroke")).hex);	
	termsListItemInner.text(term.term);

	termsListItemCloseImg.click(function() {
		removeTerm(term.term);
	});
	
	termsListItemCloseImg.hover(
			function(){termsListItemCloseImg.attr("src", "static/images/icons/close_hover.png");},
			function(){termsListItemCloseImg.attr("src", "static/images/icons/close.png");});
	
	$("#termsList").append(termsListItem);
}

function onHoverInTerm() {

	var termId = ""; 

	// If the hover element was... 
	if (this.bar) {
		termId = this.bar.id;
	} else {
		termId = this.id;
	}

	var termId = termId.split("_");
	if (termId.length === 2) {
		var term = termId[1];

		// Highlight the term in the terms list...
		hoveredListItem = $("#termListItem_" + term);
		hoveredListItem.addClass("termsListItem-hover");
		
		// ...and the totals bar chart...
		bar = barChart.getById("termBarItem_" + term);
		hoveredBarChartFlag = barChart.popup(10, bar.y, bar.title || "", "right").insertAfter(bar);

		// ...and the time series graph.
		hoveredGraphItemGlow = graph.getById("termLineItem_" + term).glow();
	}	
}

function onHoverOutTerm() {

	if (hoveredListItem) {
		hoveredListItem.removeClass("termsListItem-hover");
	}	

	if (hoveredBarChartFlag) {
		hoveredBarChartFlag.remove();
	}

	if (hoveredGraphItemGlow) {
		hoveredGraphItemGlow.remove();
	}
	
	hoveredListItem = null;
	hoveredBarChartFlag = null;
	hoveredGraphItemGlow = null;	
}

//****************************************************************************
//*                                                                          *
//*  Misc                                                                    *
//*                                                                          *
//****************************************************************************

var month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" ];

function getMonth(date) {
	return (date) ? month_names[date.getMonth()] : "";
}

function formatDateShort(date) {
	if (date) {
		return getMonth(date) + ", " + date.getFullYear();
	}

	return "";
}

function formatNumber(number) {
	// From: http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}


function updateDownloadLink() {

	$("#downloadDataLink").hide();
	currState = getCurrentState();

	if (currState.terms && currState.terms.length > 0) {
		
		var requestData = getRequestData(currState);
		requestData["download"] = true;
		
		var downloadUrl = URL_GET_TERM_COUNTS + "?" + decodeURIComponent($.param(requestData));

		$("#downloadDataLink").attr("href", downloadUrl);
		$("#downloadDataLink").show();
	}
}

$(document).ready(function() {	
	// Setup buttons
	$("#addTermButton").button({icons: {primary:"ui-icon-search" }, text: false});
	$("#addTermButton").click(addTerm);

	// Setup graphs and charts
	graph = Raphael("timeSeriesGraph", 900, 350);
	barChart = Raphael("totalsBarChart", 250, 75);

	// Get latest state and register for future state changes
	onStateChange();
	window.onhashchange = function() {
		onStateChange();
	};

	setupAutocomplete();
});
