(function () {

  var LANGS = ['en'];
  var VERSIONS_URL = 'https://ksqldb.io/versions/handleVersions.js';

  function selectVersion(version) {
    var segs = window.location.pathname.split('/');

    // replace the part of the path after the language
    var updated = segs.reduce(function(acc, seg) {
      if (LANGS.indexOf(acc.prev) > -1) {
        acc.segs.push(version);
        acc.foundLang = true;
      } else {
        acc.segs.push(seg);
      }
      acc.prev = seg;
      return acc;
    }, {prev: '', segs: [], foundLang: false});

    const updatedUrl = updated.segs.join('/');

    if (updated.foundLang) {
      window.location.replace(updatedUrl);
    } else {
      console.warn('Version selector could not infer a new url. Are you running in a local development environment?', version, updated);
    }
  }

  function versionSelector(config) {
    var nav = document.querySelector('nav');
    var select = document.createElement('select');

    config.versions.forEach(function(v) {
      var option = document.createElement('option');
      option.value = v;
      option.innerText = v;
      option.selected = v === config.defaultVersion;

      select.append(option);
    });

    select.addEventListener('change', function (e) {
      selectVersion(e.target.value);
    });

    nav.append(select);
    select.classList.add('version');
  }

  window.handleVersions = function(json) {
    versionSelector(JSON.parse(json));
  };

  function init() {
    var s = document.createElement('script');
    s.src = VERSIONS_URL;
    s.async = true;
    document.body.append(s);
  }

  init();

})();
