/* news_data : https://github.com/lbracken/news_data
   :license: MIT, see LICENSE for more details.
*/
var URL_GET_DB_STATS = "../get_db_stats";
var graphWidth = 365;
var graphHeight = 175;
var xPos = 30;
var yPos = 0;
var xAxisStep = 6;
var yAxisStep = 5;

var graph_raw_articles = null;
var graph_parsed_articles = null;
var graph_parsed_articles_size_raw = null;
var graph_parsed_articles_size_parsed = null;
var graph_parsed_articles_size_raw_avgminmax = null;
var graph_parsed_articles_size_parsed_avgminmax = null;
var graph_parsed_articles_size_ratio_avgminmax = null;
var graph_analyzed_articles = null;
var graph_analyzed_articles_unique_terms = null;
var graph_analyzed_articles_unique_terms_avgminmax = null;
var graph_analyzed_articles_total_terms = null;
var graph_analyzed_articles_total_terms_avgminmax = null;


function onDBStatsResponse(data) {
	
	$("#loading").fadeOut();
	$("#dbStatsContent").fadeIn();

	if(data) {
		updateTotalCounts(data);
		updateGraphs(data);
	}
}


function updateTotalCounts(data) {
	$("#total_raw_articles").text(formatNumber(data.total_raw_articles));
	$("#total_parsed_articles").text(formatNumber(data.total_parsed_articles));
    $("#total_analyzed_articles").text(formatNumber(data.total_analyzed_articles));
    $("#total_metric_data_daily").text(formatNumber(data.total_metric_data_daily));
    $("#total_metric_data_monthly").text(formatNumber(data.total_metric_data_monthly));
}

function updateGraphs(data) {

	graphWidth = graphWidth - xPos;
	graphHeight = graphHeight - 10;

	// Raw Articles
	populateGraph(
		graph_raw_articles,
		data.raw_articles.count);	

	// Parsed Articles
	populateGraph(
		graph_parsed_articles,
		data.parsed_articles.count);

	populateGraph(
		graph_parsed_articles_size_raw,
		data.parsed_articles.size_raw_sum);

	populateGraph(
		graph_parsed_articles_size_parsed,
		data.parsed_articles.size_parsed_sum);
	
	populateGraph(
		graph_parsed_articles_size_raw_avgminmax,
		data.parsed_articles.size_raw_avg,
		data.parsed_articles.size_raw_min,
		data.parsed_articles.size_raw_max);

	populateGraph(graph_parsed_articles_size_parsed_avgminmax,
		data.parsed_articles.size_parsed_avg,
		data.parsed_articles.size_parsed_min,
		data.parsed_articles.size_parsed_max);

	populateGraph(
		graph_parsed_articles_size_ratio_avgminmax,
		data.parsed_articles.size_ratio_avg,
		data.parsed_articles.size_ratio_min,
		data.parsed_articles.size_ratio_max);

	// Analyzed Articles
	populateGraph(
		graph_analyzed_articles,
		data.analyzed_articles.count);

	populateGraph(
		graph_analyzed_articles_unique_terms,
		data.analyzed_articles.unique_terms_sum);
	
	populateGraph(
		graph_analyzed_articles_unique_terms_avgminmax,
		data.analyzed_articles.unique_terms_avg,
		data.analyzed_articles.unique_terms_min,
		data.analyzed_articles.unique_terms_max);

	populateGraph(
		graph_analyzed_articles_total_terms,
		data.analyzed_articles.total_terms_sum);
	
	populateGraph(
		graph_analyzed_articles_total_terms_avgminmax,
		data.analyzed_articles.total_terms_avg,
		data.analyzed_articles.total_terms_min,
		data.analyzed_articles.total_terms_max);
}

function populateGraph(graph, line, line_min, line_max) {

	var opts = {
		axis: "0 0 1 1",
		axisxstep: xAxisStep,
		axisystep: yAxisStep,
		smooth: true
		//shade: true, nostroke: true
	};

	var showMax = !$("#hide_max_cb").prop("checked");
	var showMin = !$("#hide_min_cb").prop("checked");

	var valuesX = [];
	for (var i = 0; i <= line.length+1; i++) {
		valuesX.push(i);
	};

	var valuesY = [line];
	if (line_min && showMin) { valuesY.push(line_min); }
	if (line_max && showMax) { valuesY.push(line_max); }

	var graphLines = graph.linechart(xPos, yPos, graphWidth, graphHeight, valuesX, valuesY, opts);

	// Format the y-axis
	var yText = graphLines.axis[1].text.items;
	for(var i in yText) {
		var value = yText[i].attr("text");
		if(value >= 1000000) {
			value = formatNumber(value / 1000000) + "M";
		} else if (value > 1000) {
			value = formatNumber(value / 1000) + "k";
		}

		yText[i].attr({'text' : value});
	}
}


function formatNumber(number) {
	// From: http://stackoverflow.com/questions/2901102/how-to-print-a-number-with-commas-as-thousands-separators-in-javascript
    return number.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// From: http://james.padolsey.com/javascript/parsing-urls-with-the-dom/
// This function creates a new anchor element and uses location
// properties (inherent) to get the desired URL data. Some String
// operations are used (to normalize results across browsers).
function parseURL(url) {
    var a =  document.createElement('a');
    a.href = url;
    return {
        source: url,
        protocol: a.protocol.replace(':',''),
        host: a.hostname,
        port: a.port,
        query: a.search,
        params: (function(){
            var ret = {},
                seg = a.search.replace(/^\?/,'').split('&'),
                len = seg.length, i = 0, s;
            for (;i<len;i++) {
                if (!seg[i]) { continue; }
                s = seg[i].split('=');
                ret[s[0]] = s[1];
            }
            return ret;
        })(),
        file: (a.pathname.match(/\/([^\/?#]+)$/i) || [,''])[1],
        hash: a.hash.replace('#',''),
        path: a.pathname.replace(/^([^\/])/,'/$1'),
        relative: (a.href.match(/tps?:\/\/[^\/]+(.+)/) || [,''])[1],
        segments: a.pathname.replace(/^\//,'').split('/')
    };
}

function initSettings() {
	var settings = parseURL(window.location);
	settings = settings.params;

	var start_time_str = "01/01/2000";
	if (settings.start_time) {
		var d = new Date(settings.start_time * 1000);
		start_time_str = (d.getMonth()+1) + "/" + d.getDate() + "/" + d.getFullYear();
	}

	var end_time_str = "12/12/2012";
	if (settings.end_time) {
		var d = new Date(settings.end_time * 1000);
		end_time_str = (d.getMonth()+1) + "/" + d.getDate() + "/" + d.getFullYear();
	}


	$("#start_time_txt").val(start_time_str);
	$("#end_time_txt").val(end_time_str);
	$("#hide_max_cb").prop("checked", "true" === settings.hide_max);
	$("#hide_min_cb").prop("checked", "true" === settings.hide_min);

	// Setup TextBoxes as DateBoxes
	var datepicker_settings = {changeMonth: true, changeYear: true};
	$("#start_time_txt").datepicker(datepicker_settings);
	$("#end_time_txt").datepicker(datepicker_settings);

	$("#settings_refresh").click(function(){
		var new_settings = {
			"start_time" : getStartTime(),
			"end_time"   : getEndTime(),
			"hide_max"   : $("#hide_max_cb").prop("checked"),
			"hide_min"   : $("#hide_min_cb").prop("checked")
		};

		var query_str = $.param(new_settings);
		window.location.search = query_str;
	});
}

function getStartTime() {
	return new Date($("#start_time_txt").val()).getTime() / 1000;
}

function getEndTime() {
	return new Date($("#end_time_txt").val()).getTime() / 1000;
}


$(document).ready(function() {

	initSettings();

	// Setup the various graphs...
	graph_raw_articles = Raphael("graph_raw_articles", graphWidth, graphHeight);
	
	graph_parsed_articles = Raphael("graph_parsed_articles", graphWidth, graphHeight);
	graph_parsed_articles_size_raw = Raphael("graph_parsed_articles_size_raw", graphWidth, graphHeight);
	graph_parsed_articles_size_parsed = Raphael("graph_parsed_articles_size_parsed", graphWidth, graphHeight);
	graph_parsed_articles_size_raw_avgminmax = Raphael("graph_parsed_articles_size_raw_avgminmax", graphWidth, graphHeight);
	graph_parsed_articles_size_parsed_avgminmax = Raphael("graph_parsed_articles_size_parsed_avgminmax", graphWidth, graphHeight);
	graph_parsed_articles_size_ratio_avgminmax = Raphael("graph_parsed_articles_size_ratio_avgminmax", graphWidth, graphHeight);
	
	graph_analyzed_articles = Raphael("graph_analyzed_articles", graphWidth, graphHeight);
	graph_analyzed_articles_unique_terms = Raphael("graph_analyzed_articles_unique_terms", graphWidth, graphHeight);
	graph_analyzed_articles_unique_terms_avgminmax = Raphael("graph_analyzed_articles_unique_terms_avgminmax", graphWidth, graphHeight);
	graph_analyzed_articles_total_terms = Raphael("graph_analyzed_articles_total_terms", graphWidth, graphHeight);
	graph_analyzed_articles_total_terms_avgminmax = Raphael("graph_analyzed_articles_total_terms_avgminmax", graphWidth, graphHeight);
		
	// Call the server to get the DB stats...
	var requestData = {
		time_start : getStartTime(),
		time_end   : getEndTime()
	};

	$.getJSON(URL_GET_DB_STATS, requestData, onDBStatsResponse);
});