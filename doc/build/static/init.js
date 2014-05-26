
function initSQLPopups() {
    $('div.popup_sql').hide();
    $('a.sql_link').click(function() {
        $(this).nextAll('div.popup_sql:first').toggle();
        return false;
    });
}

var automatedBreakpoint = -1;

function initFloatyThings() {

    automatedBreakpoint = $("#docs-top-navigation-container").offset().top;

    function setNavSize() {
        $("#docs-top-navigation-container").css("width", $("#docs-container").width());
    }

    function setScroll() {
        var scrolltop = $(window).scrollTop();
        if (scrolltop >= automatedBreakpoint - 10) {
            setNavSize();
            $("#docs-top-navigation-container").addClass("automated");
            $("#docs-sidebar").addClass("automated");
            $("#docs-sidebar").css("top", $("#docs-top-navigation-container").height());
            $("#docs-body").css("margin-top", $("#docs-top-navigation-container").height());
        }
        else {
            $("#docs-top-navigation-container.automated").css("width", "");
            $("#docs-sidebar.automated").scrollTop(0);
            $("#docs-top-navigation-container").removeClass("automated");
            $("#docs-sidebar").removeClass("automated");
            $("#docs-body").css("margin-top", "");
        }
    }
    $(window).scroll(setScroll)

    $(window).resize(setNavSize());
    setScroll();
}


$(document).ready(function() {
    initSQLPopups();
    initFloatyThings();
});

