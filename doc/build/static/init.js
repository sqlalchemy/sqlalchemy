
function initSQLPopups() {
    $('div.popup_sql').hide();
    $('a.sql_link').click(function() {
        $(this).nextAll('div.popup_sql:first').toggle();
        return false;
    });
}

var automatedBreakpoint = -1;

function initFloatyThings() {

    automatedBreakpoint = $("#docs-container").position().top + $("#docs-top-navigation-container").height();

    $("#fixed-sidebar.withsidebar").addClass("preautomated");


    function setScroll() {

        var scrolltop = $(window).scrollTop();
        if (scrolltop >= automatedBreakpoint) {
            $("#fixed-sidebar.withsidebar").css("top", 5);
        }
        else {
            $("#fixed-sidebar.withsidebar").css(
                "top", $("#docs-body").offset().top - Math.max(scrolltop, 0));
        }


    }
    $(window).scroll(setScroll)

    setScroll();
}


$(document).ready(function() {
    initSQLPopups();
    if (!$.browser.mobile) {
        initFloatyThings();
    }
});

