(function () {
  function parseCharts() {
    var source = document.getElementById("qxChartsJson");
    if (!source) {
      return null;
    }
    try {
      return JSON.parse(source.textContent || "{}");
    } catch (error) {
      console.error("No se pudo leer el JSON de charts", error);
      return null;
    }
  }

  function initCharts() {
    if (typeof window.Chart === "undefined") {
      return;
    }
    var charts = parseCharts();
    if (!charts || typeof charts !== "object") {
      return;
    }
    window.Chart.defaults.font.family = "Montserrat, sans-serif";
    window.Chart.defaults.color = "#496073";
    Object.keys(charts).forEach(function (key) {
      var canvas = document.querySelector('[data-chart-key="' + key + '"]');
      if (!canvas) {
        return;
      }
      var config = charts[key];
      if (!config || !config.type || !config.data) {
        return;
      }
      var context = canvas.getContext("2d");
      if (!context) {
        return;
      }
      new window.Chart(context, config);
    });
  }

  function initReviewFilters() {
    var toolbar = document.querySelector("[data-review-toolbar]");
    if (!toolbar) {
      return;
    }
    var rows = Array.prototype.slice.call(document.querySelectorAll("[data-review-row]"));
    if (!rows.length) {
      return;
    }
    var buttons = Array.prototype.slice.call(toolbar.querySelectorAll("[data-review-filter]"));
    var nextButton = document.querySelector("[data-next-discrepancy]");

    function isVisible(row, mode) {
      if (mode === "discrepancy") {
        return row.dataset.hasDiscrepancy === "true";
      }
      if (mode === "pending") {
        return row.dataset.reviewStatus === "REVIEW";
      }
      if (mode === "ready") {
        return row.dataset.reviewStatus === "READY";
      }
      return true;
    }

    function applyFilter(mode) {
      rows.forEach(function (row) {
        row.hidden = !isVisible(row, mode);
      });
      buttons.forEach(function (button) {
        button.classList.toggle("is-active", button.dataset.reviewFilter === mode);
      });
    }

    buttons.forEach(function (button) {
      button.addEventListener("click", function () {
        applyFilter(button.dataset.reviewFilter || "all");
      });
    });

    if (nextButton) {
      nextButton.addEventListener("click", function () {
        var nextRow = rows.find(function (row) {
          return !row.hidden && row.dataset.hasDiscrepancy === "true";
        });
        if (nextRow) {
          nextRow.scrollIntoView({ behavior: "smooth", block: "center" });
          nextRow.classList.add("is-jump-target");
          window.setTimeout(function () {
            nextRow.classList.remove("is-jump-target");
          }, 1400);
        }
      });
    }

    applyFilter("all");
  }

  function initCollapsibles() {
    var items = Array.prototype.slice.call(document.querySelectorAll("details[data-collapsible-id]"));
    if (!items.length || typeof window.localStorage === "undefined") {
      return;
    }
    items.forEach(function (item) {
      var key = "qxCollapse:" + item.dataset.collapsibleId;
      var saved = window.localStorage.getItem(key);
      if (saved === "open") {
        item.open = true;
      }
      if (saved === "closed") {
        item.open = false;
      }
      item.addEventListener("toggle", function () {
        window.localStorage.setItem(key, item.open ? "open" : "closed");
      });
    });
  }

  function initExpandableRows() {
    var toggles = Array.prototype.slice.call(document.querySelectorAll("[data-row-toggle]"));
    if (!toggles.length) {
      return;
    }
    toggles.forEach(function (button) {
      var targetId = button.dataset.rowTarget;
      if (!targetId) {
        return;
      }
      var target = document.getElementById(targetId);
      if (!target) {
        return;
      }
      function sync() {
        var expanded = !target.hidden;
        button.setAttribute("aria-expanded", expanded ? "true" : "false");
        if (button.dataset.labelOpen && button.dataset.labelClose) {
          button.textContent = expanded ? button.dataset.labelClose : button.dataset.labelOpen;
        }
      }
      sync();
      button.addEventListener("click", function () {
        target.hidden = !target.hidden;
        sync();
      });
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    initCharts();
    initReviewFilters();
    initCollapsibles();
    initExpandableRows();
  });
})();
