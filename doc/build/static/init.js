
function initSQLPopups() {
    $('div.popup_sql').hide();
    $('a.sql_link').click(function() {
        $(this).nextAll('div.popup_sql:first').toggle();
        return false;
    });
}

var automatedBreakpoint = -1;

function initFloatyThings() {

    automatedBreakpoint = $("#docs-top-navigation-container").position().top;

    parentOffset = $("#docs-container").parent().position().top - $("#docs-container").position().top;
    containerHeight = $("#docs-top-navigation-container").height();

    sidebar = $("#docs-sidebar").position();
    if (sidebar) {
        margin = sidebar.top - (automatedBreakpoint + containerHeight);
    }
    else {
        margin = 0;
    }
    autoOffset = containerHeight + margin - parentOffset;

    $("#docs-sidebar").addClass("preautomated");
    $("#docs-container").addClass("preautomated");


    function setNavSize() {
        $("#docs-top-navigation-container").css("width", $("#docs-container").width());
    }

    function setScroll() {

        var scrolltop = $(window).scrollTop();
        if (scrolltop >= automatedBreakpoint) {
            setNavSize();
            $("#docs-top-navigation-container").addClass("automated");
            $("#docs-sidebar").addClass("automated");
            $("#docs-container").addClass("automated");
            $("#docs-body").css("margin-top", autoOffset);
        }
        else {
            $("#docs-top-navigation-container.automated").css("width", "");
            $("#docs-sidebar.automated").scrollTop(0);
            $("#docs-top-navigation-container").removeClass("automated");
            $("#docs-container").removeClass("automated");
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
    if (!$.browser.mobile) {
        initFloatyThings();
    }
});

