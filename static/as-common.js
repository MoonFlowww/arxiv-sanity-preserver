// various JS utilities shared by all templates

// helper function so that we can access keys in url bar
var QueryString = function () {
    // This function is anonymous, is executed immediately and
    // the return value is assigned to QueryString!
    var query_string = {};
    var query = window.location.search.substring(1);
    var vars = query.split("&");
    for (var i = 0; i < vars.length; i++) {
        var pair = vars[i].split("=");
        // If first entry with this name
        if (typeof query_string[pair[0]] === "undefined") {
            query_string[pair[0]] = decodeURIComponent(pair[1]);
            // If second entry with this name
        } else if (typeof query_string[pair[0]] === "string") {
            var arr = [query_string[pair[0]], decodeURIComponent(pair[1])];
            query_string[pair[0]] = arr;
            // If third or later entry with this name
        } else {
            query_string[pair[0]].push(decodeURIComponent(pair[1]));
        }
    }
    return query_string;
}();

var layoutSelection = 'double';

function applyLayoutClass(layoutChoice) {
    var rtable = $('#rtable');
    if (!rtable.length) {
        return layoutSelection;
    }

    var choice = layoutChoice === 'single' ? 'single' : 'double';
    layoutSelection = choice;

    rtable.attr('data-layout', layoutSelection);
    rtable.removeClass('layout-single layout-double').addClass('layout-' + layoutSelection);
    return layoutSelection;
}

function initLayoutToggle(buttonSelector) {
    var layoutButtons = $(buttonSelector);
    var rtable = $('#rtable');
    if (!layoutButtons.length || !rtable.length) {
        return;
    }

    var savedLayout = null;
    var canPersistLayout = false;
    try {
        canPersistLayout = typeof window !== 'undefined' && 'localStorage' in window;
        savedLayout = canPersistLayout ? localStorage.getItem('preferredLayout') : null;
    } catch (e) {
    }

    var initialChoice = savedLayout || rtable.data('layout') || layoutSelection;

    function updateToggleState(selection) {
        layoutButtons.attr('aria-pressed', 'false').removeClass('active');
        layoutButtons.filter('[data-layout="' + selection + '"]').attr('aria-pressed', 'true').addClass('active');
    }

    layoutButtons.on('click', function () {
        var choice = $(this).data('layout');
        var appliedChoice = applyLayoutClass(choice);
        if (canPersistLayout) {
            localStorage.setItem('preferredLayout', appliedChoice);
        }
        updateToggleState(appliedChoice);
    });

    var appliedInitial = applyLayoutClass(initialChoice);
    updateToggleState(appliedInitial);
}

function updateRecomputeBadge(isFinished) {
    var badge = $('#recompute-badge');
    if (!badge.length) {
        return;
    }

    var finished = Boolean(isFinished);
    badge.toggleClass('recompute-badge-finished', finished);
    badge.attr('aria-hidden', finished ? 'true' : 'false');
}

function initHealthPanel() {
    var button = $('#health-state-button');
    var panel = $('#health-state-panel');
    if (!button.length || !panel.length) {
        return;
    }

    function setPanelVisible(visible) {
        panel.attr('aria-hidden', visible ? 'false' : 'true');
        panel.toggleClass('visible', visible);
        button.attr('aria-expanded', visible ? 'true' : 'false');
    }

    button.on('click', function (event) {
        event.stopPropagation();
        var isVisible = panel.hasClass('visible');
        setPanelVisible(!isVisible);
    });

    $(document).on('click', function (event) {
        if (!panel.has(event.target).length && event.target !== button[0]) {
            setPanelVisible(false);
        }
    });

    function formatUptime(seconds) {
        if (typeof seconds !== 'number') {
            return 'Unknown';
        }
        var minutes = Math.floor(seconds / 60);
        var hours = Math.floor(minutes / 60);
        var days = Math.floor(hours / 24);
        if (days > 0) {
            return days + 'd ' + (hours % 24) + 'h';
        }
        if (hours > 0) {
            return hours + 'h ' + (minutes % 60) + 'm';
        }
        return minutes + 'm';
    }

    function renderErrors(errors) {
        var list = $('#health-errors-list');
        list.empty();
        if (!errors || !errors.length) {
            list.append('<li class="health-empty">No errors reported.</li>');
            return;
        }
        errors.slice(0, 50).forEach(function (entry) {
            var label = entry.source ? entry.source.toUpperCase() : 'ERROR';
            var message = entry.message || 'Unknown error';
            var timestamp = entry.timestamp ? new Date(entry.timestamp * 1000).toLocaleString() : '';
            var item = $('<li></li>');
            item.append('<span class="health-error-source">' + label + '</span>');
            item.append('<span class="health-error-message">' + message + '</span>');
            if (timestamp) {
                item.append('<span class="health-error-time">' + timestamp + '</span>');
            }
            list.append(item);
        });
    }

    function updateHealthPanel(data) {
        var db = data.database || {};
        var dbStatus = 'DB: ' + (db.selected_db || 'Unknown');
        if (typeof db.total_papers === 'number') {
            dbStatus += ' • Papers: ' + db.total_papers;
        }
        if (typeof db.downloaded_percent === 'number') {
            dbStatus += ' • PDFs: ' + db.downloaded_percent + '%';
        }
        $('#health-db-status').text(dbStatus);

        var site = data.site || {};
        var siteStatus = 'Uptime: ' + formatUptime(site.uptime_seconds);
        if (site.recompute_status) {
            siteStatus += ' • Recompute: ' + site.recompute_status;
        }
        if (site.recompute_message) {
            siteStatus += ' • ' + site.recompute_message;
        }
        $('#health-site-status').text(siteStatus);

        var network = data.network || {};
        var networkStatus = network.reachable ? 'Online' : 'Offline';
        if (network.ip) {
            networkStatus += ' • IP: ' + network.ip;
        }
        if (network.error) {
            networkStatus += ' • ' + network.error;
        }
        $('#health-network-status').text(networkStatus);
    }

    function fetchHealth() {
        $.getJSON('/status/health', function (data) {
            updateHealthPanel(data);
        });
        $.getJSON('/status/errors', function (data) {
            renderErrors(data.errors);
        });
    }

    fetchHealth();
    setInterval(fetchHealth, 5000);
}

function reportClientError(message, context) {
    if (!message) {
        return;
    }
    try {
        $.ajax({
            url: '/status/report_error',
            method: 'POST',
            contentType: 'application/json',
            data: JSON.stringify({
                source: 'client',
                message: message,
                context: context || {},
            }),
        });
    } catch (e) {
    }
}

window.addEventListener('error', function (event) {
    var message = event.message || 'Unknown client error';
    reportClientError(message, {
        filename: event.filename,
        lineno: event.lineno,
        colno: event.colno,
    });
});

window.addEventListener('unhandledrejection', function (event) {
    var message = event.reason ? event.reason.toString() : 'Unhandled promise rejection';
    reportClientError(message, {});
});


function updateRecomputeBadge(isFinished) {
    var badge = $('#recompute-badge');
    if (!badge.length) {
        return;
    }

    var finished = Boolean(isFinished);
    badge.toggleClass('recompute-badge-finished', finished);
    badge.attr('aria-hidden', finished ? 'true' : 'false');
}



function jq(myid) {
    return myid.replace(/(:|\.|\[|\]|,)/g, "\\$1");
} // for dealing with ids that have . in them

function build_ocoins_str(p) {
    var ocoins_info = {
        "ctx_ver": "Z39.88-2004",
        "rft_val_fmt": "info:ofi/fmt:kev:mtx:journal",
        "rfr_id": "info:sid/arxiv-sanity.com:arxiv-sanity",

        "rft_id": p.link,
        "rft.atitle": p.title,
        "rft.jtitle": "arXiv:" + p.pid + " [" + p.category.substring(0, p.category.indexOf('.')) + "]",
        "rft.date": p.published_time,
        "rft.artnum": p.pid,
        "rft.genre": "preprint",

        // NB: Stolen from Dublin Core; Zotero understands this even though it's
        // not part of COinS
        "rft.description": p.abstract,
    };
    ocoins_info = $.param(ocoins_info);
    ocoins_info += "&" + $.map(p.authors, function (a) {
        return "rft.au=" + encodeURIComponent(a);
    }).join("&");

    return ocoins_info;
}

function build_authors_html(authors) {
    var res = '';
    for (var i = 0, n = authors.length; i < n; i++) {
        var link = '/search?q=' + authors[i].replace(/ /g, "+");
        res += '<a href="' + link + '">' + authors[i] + '</a>';
        if (i < n - 1) res += ', ';
    }
    return res;
}

function build_categories_html(tags) {
    var res = '';
    for (var i = 0, n = tags.length; i < n; i++) {
        var link = '/search?q=' + tags[i].replace(/ /g, "+");
        res += '<a href="' + link + '">' + tags[i] + '</a>';
        if (i < n - 1) res += ' | ';
    }
    return res;
}

function strip_version(pidv) {
    var lst = pidv.split('v');
    return lst[0];
}

// populate papers into #rtable
// we have some global state here, which is gross and we should get rid of later.
var pointer_ix = 0; // points to next paper in line to be added to #rtable
var showed_end_msg = false;

function addPapers(num, dynamic) {
    if (papers.length === 0) {
        return true;
    } // nothing to display, and we're done

    var root = d3.select("#rtable");

    var base_ix = pointer_ix;
    for (var i = 0; i < num; i++) {
        var ix = base_ix + i;
        if (ix >= papers.length) {
            if (!showed_end_msg) {
                if (ix >= numresults) {
                    var msg = 'Results complete.';
                } else {
                    var msg = 'You hit the limit of number of papers to show in one result.';
                }
                root.append('div').classed('msg', true).html(msg);
                showed_end_msg = true;
            }
            break;
        }
        pointer_ix++;

        var p = papers[ix];
        var div = root.append('div').classed('apaper', true).attr('id', p.pid);

        // Generate OpenURL COinS metadata element -- readable by Zotero, Mendeley, etc.
        var ocoins_span = div.append('span').classed('Z3988', true).attr('title', build_ocoins_str(p));

        var tdiv = div.append('div').classed('paperdesc', true);
        tdiv.append('span').classed('ts', true).append('a').attr('href', p.link).attr('target', '_blank').html(p.title);
        tdiv.append('br');
        tdiv.append('span').classed('as', true).html(build_authors_html(p.authors));
        tdiv.append('br');
        tdiv.append('span').classed('ds', true).html(p.published_time);
        if (p.originally_published_time !== p.published_time) {
            tdiv.append('span').classed('ds2', true).html('(v1: ' + p.originally_published_time + ')');
        }
        tdiv.append('span').classed('cs', true).html(build_categories_html(p.tags));
        tdiv.append('br');
        var metaRow = tdiv.append('div').classed('paper-meta', true);

        var hasComment = typeof p.comment === 'string' && p.comment.trim() !== '';
        if (hasComment) {
            metaRow.append('span').classed('ccs', true).text(p.comment);
        }

        var metrics = metaRow.append('div').classed('paper-metrics', true);
        var impactScore = Number(p.impact_score);
        if (Number.isFinite(impactScore)) {
            var normalizedScore = Math.abs(impactScore) < 0.005 ? 0 : impactScore;
            var popularityClass = 'popularity-0';
            if (normalizedScore >= 3) {
                popularityClass = 'popularity-3';
            } else if (normalizedScore >= 2) {
                popularityClass = 'popularity-2';
            } else if (normalizedScore >= 1) {
                popularityClass = 'popularity-1';
            }
            var formattedScore = normalizedScore.toFixed(2);
            metrics.append('span').classed('paper-popularity ' + popularityClass, true).text('Score: ' + formattedScore);
        }
        if (typeof p.citation_count !== 'undefined') {
            metrics.append('span').classed('cit', true).text('Citations: ' + p.citation_count);
        }
        if (metrics.node().childElementCount === 0) {
            metrics.remove();
        }

        // action items for each paper
        var ldiv = div.append('div').classed('dllinks', true);
        // show raw arxiv id
        ldiv.append('span').classed('spid', true).html(p.pid);
        // access PDF of the paper
        var pdf_link = p.link.replace("abs", "pdf"); // convert from /abs/ link to /pdf/ link. url hacking. slightly naughty
        if (pdf_link === p.link) {
            var pdf_url = pdf_link
        } // replace failed, lets fall back on arxiv landing page
        else {
            var pdf_url = pdf_link + '.pdf';
        }
        ldiv.append('a').attr('href', pdf_url).attr('target', '_blank').html('pdf');

        // rank by tfidf similarity
        ldiv.append('br');
        var similar_span = ldiv.append('span').classed('sim', true).attr('id', 'sim' + p.pid).html('show similar');
        similar_span.on('click', function (pid) { // attach a click handler to redirect for similarity search
            return function () {
                window.location.replace('/' + pid);
            }
        }(p.pid)); // closer over the paper id

        // var review_span = ldiv.append('span').classed('sim', true).attr('style', 'margin-left:5px; padding-left: 5px; border-left: 1px solid black;').append('a').attr('href', 'http://www.shortscience.org/paper?bibtexKey='+p.pid).html('review');
        ldiv.append('br');

        var lib_state_img = p.in_library === 1 ? 'static/saved.png' : 'static/save.png';
        var saveimg = ldiv.append('img').attr('src', lib_state_img)
            .classed('save-icon', true)
            .attr('title', 'toggle save paper to library')
            .attr('id', 'lib' + p.pid);
        // attach a handler for in-library toggle
        saveimg.on('click', function (pid, elt) {
            return function () {
                // issue the post request to the server
                $.post("/libtoggle", {pid: pid})
                    .done(function (data) {
                        // toggle state of the image to reflect the state of the server, as reported by response
                        if (data === 'ON') {
                            elt.attr('src', 'static/saved.png');
                        } else if (data === 'OFF') {
                            elt.attr('src', 'static/save.png');
                        }
                    });
            }
        }(p.pid, saveimg)); // close over the pid and handle to the image

        div.append('div').attr('style', 'clear:both');

        if (typeof p.img !== 'undefined') {
            div.append('div').classed('animg', true).append('img').attr('src', p.img);
        }

        if (typeof p.abstract !== 'undefined') {
            var abdiv = div.append('span').classed('tt', true).html(p.abstract);
            if (dynamic) {
                MathJax.Hub.Queue(["Typeset", MathJax.Hub, abdiv[0]]); //typeset the added paper
            }
        }


        // create the tweets
        if (ix < tweets.length) {
            var ix_tweets = tweets[ix].tweets; // looks a little weird, i know
            var tdiv = div.append('div').classed('twdiv', true);
            var tcontentdiv = div.append('div').classed('twcont', true);
            tdiv.append('div').classed('tweetcount', true).text(tweets[ix].num_tweets + ' tweets:');
            for (var j = 0, m = ix_tweets.length; j < m; j++) {
                var t = ix_tweets[j];
                var border_col = t.ok ? '#3c3' : '#fff'; // distinguish non-boring tweets visually making their border green
                var timgdiv = tdiv.append('img').classed('twimg', true).attr('src', t.image_url)
                    .attr('style', 'border: 2px solid ' + border_col + ';');
                var act_fun = function (elt, txt, tname, tid, imgelt) {  // mouseover handler: show tweet text.
                    return function () {
                        elt.attr('style', 'display:block;'); // make visible
                        elt.html(''); // clear it
                        elt.append('div').append('a').attr('href', 'https://twitter.com/' + tname + '/status/' + tid).attr('target', '_blank')
                            .attr('style', 'font-weight:bold; color:#05f; text-decoration:none;').text('@' + tname + ':'); // show tweet source
                        elt.append('div').text(txt) // show tweet text
                        imgelt.attr('style', 'border: 2px solid #05f;');
                    }
                }(tcontentdiv, t.text, t.screen_name, t.id, timgdiv)
                timgdiv.on('mouseover', act_fun);
                timgdiv.on('click', act_fun);
                timgdiv.on('mouseout', function (elt, col) {
                    return function () {
                        elt.attr('style', 'border: 2px solid ' + col + ';');
                    }
                }(timgdiv, border_col));
            }
        }

        if (render_format == 'paper' && ix === 0) {
            // lets insert a divider/message
            div.append('div').classed('paperdivider', true).html('Most similar papers:');
        }
    }

    return pointer_ix >= papers.length; // are we done?
}

function timeConverter(UNIX_timestamp) {
    var a = new Date(UNIX_timestamp * 1000);
    var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    var year = a.getFullYear();
    var month = months[a.getMonth()];
    var date = a.getDate().toString();
    var hour = a.getHours().toString();
    var min = a.getMinutes().toString();
    var sec = a.getSeconds().toString();
    if (hour.length === 1) {
        hour = '0' + hour;
    }
    if (min.length === 1) {
        min = '0' + min;
    }
    if (sec.length === 1) {
        sec = '0' + sec;
    }
    var time = date + ' ' + month + ' ' + year + ', ' + hour + ':' + min + ':' + sec;
    return time;
}
