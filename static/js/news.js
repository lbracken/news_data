/* news_data : https://github.com/lbracken/news_data
   :license: MIT, see LICENSE for more details.
*/
var URL_GET_TERM_COUNTS = "get_term_counts";

function addTerm() {
	// TODO...
}

$(document).ready(function() {	
	// Setup buttons
	$("#addTermButton").button({icons: {primary:"ui-icon-search" }, text: false});
	$("#addTermButton").click(addTerm);
});
