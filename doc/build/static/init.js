
function initSQLPopups() {
    $('div.popup_sql').hide();
    $('a.sql_link').click(function() {
        $(this).nextAll('div.popup_sql:first').toggle();
        return false;
    });
}

function initFloatyThings() {

    // we use a "fixed" positioning for the sidebar regardless
    // of whether or not we are moving with the page or not because
    // we want it to have an independently-moving scrollbar at all
    // times.
    // this unfortunately means we either have to keep it steady across
    // page scrolls or deal with the fact that the text is flowing
    // under it in some resize/side-scroll scenarios.

    var automatedBreakpoint = $("#docs-container").position().top +
        $("#docs-top-navigation-container").height();

    var docsBodyOffset = $("#docs-body").offset().top;

    // this turns on the whole thing, without this
    // we are in graceful degradation assuming no JS
    $("#fixed-sidebar.withsidebar").addClass("preautomated");

    function setScroll() {
        var scrolltop = $(window).scrollTop();
        var fix = scrolltop >= automatedBreakpoint;

        // when page is scrolled down past the top headers,
        // sidebar stays fixed vertically
        if (fix) {
            $("#fixed-sidebar.withsidebar").css("top", 5);
        }
        else if (scrolltop < 0) {
            // special trickery to deal with safari vs. chrome
            // acting differently in this case, while avoiding using jquery's
            // weird / slow? offset() setter
            if ($("#fixed-sidebar.withsidebar").offset().top != docsBodyOffset) {
                $("#fixed-sidebar.withsidebar").css(
                    "top", docsBodyOffset - scrolltop);
            }
        }
        else {
            $("#fixed-sidebar.withsidebar").css(
                "top", docsBodyOffset - scrolltop);
        }

        // adjusting left scroll is also an option,
        // but doesn't seem to be worth it, safari is the only browser
        // that shows much of a change, and overall the adjustment here
        // is jerky and error-prone esp. on lesser browsers like safari ipad.
        // looking at our "mentor" documentation, they don't do this;
        // they just have the whole layout such that you don't really notice
        // the horizontal squeezing as much (nav is on the right, they don't
        // have a border around the text making it obvious).
    }
    $(window).scroll(setScroll);
    setScroll();
}


$(document).ready(function() {
    initSQLPopups();
    if (!$.browser.mobile) {
        initFloatyThings();
    }
});

