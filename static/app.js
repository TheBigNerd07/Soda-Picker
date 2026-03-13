const clock = document.getElementById("clock");

if (clock) {
  const timezone = clock.dataset.timezone;
  const initialIso = clock.dataset.iso;

  if (timezone && initialIso) {
    let current = new Date(initialIso);
    const formatter = new Intl.DateTimeFormat("en-US", {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
      timeZone: timezone,
    });

    const render = () => {
      clock.textContent = formatter.format(current);
      current = new Date(current.getTime() + 1000);
    };

    render();
    window.setInterval(render, 1000);
  }
}
