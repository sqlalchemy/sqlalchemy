
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

    left = $("#fixed-sidebar.withsidebar").offset()
    if (left) {
        left = left.left;
    } // otherwise might be undefined

    // we use a "fixed" positioning for the sidebar regardless
    // of whether or not we are moving with the page or not because
    // we want it to have an independently-moving scrollbar at all
    // times.  Otherwise, keeping it with plain positioning before the
    // page has scrolled works more smoothly on safari, IE
    $("#fixed-sidebar.withsidebar").addClass("preautomated");

    function setScroll(event) {
        var scrolltop = $(window).scrollTop();
        if (scrolltop < 0) {
            // safari does this
            $("#fixed-sidebar.withsidebar").css(
                "top", $("#docs-body").offset().top - scrolltop);
        }
        else if (scrolltop >= automatedBreakpoint) {
            $("#fixed-sidebar.withsidebar").css("top", 5);
        }
        else {
          $("#fixed-sidebar.withsidebar").css(
                "top", $("#docs-body").offset().top - Math.max(scrolltop, 0));
        }

        var scrollside = $(window).scrollLeft();
        // more safari crap, side scrolling
        $("#fixed-sidebar.withsidebar").css("left", left - scrollside);
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

