
function togglePopbox(id, show, hide) {
	var link = document.getElementById(id + "_link");
	var div = document.getElementById(id + "_div");
	if (div.style.display == 'block') {
		div.style.display = 'none';
		if (link) {
		    link.firstChild.nodeValue = show;
		}
	}
	else if (div.style.display == 'none') {
		div.style.display = 'block';
		if (link) {
		link.firstChild.nodeValue = hide;
		}
	}
}

