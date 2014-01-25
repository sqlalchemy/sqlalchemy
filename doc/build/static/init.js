
function initSQLPopups() {
    $('div.popup_sql').hide();
    $('a.sql_link').click(function() {
        $(this).nextAll('div.popup_sql:first').toggle();
        return false;
    });
}

/*function initFloatyThings() {
	$("dl.function, dl.class, dl.method, dl.attr, dl.data").each(function(idx, elem) {
		$(elem).prepend("<div class='floatything'>" + $(elem).contents("dt").attr('id')+ "</div>");
	});
}*/

$(document).ready(function() {
    initSQLPopups();
    /*initFloatyThings();*/
});

