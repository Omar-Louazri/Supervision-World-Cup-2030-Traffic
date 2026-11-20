# World Cup 2030 presentation guide

The live dashboard opens on a Three.js model of a fictional Rabat stadium. The scene shows the pitch, stands, three gates, and a nearby transit stop. Each marker uses the coordinates of a synthetic stadium access or transit validation event from the latest burst confirmed in Redis.

## Set up and rehearse

Run these commands from the repository root with Docker running and `uv` and Java installed. The first run needs network access to pull images, install dependencies and cache Spark’s Kafka connector. [Quick start](../README.md#quick-start)

```bash
./present.sh upgrade
./present.sh
```

Keep the terminal on the presenter’s screen. Open the dashboard on the projected screen. Its **Présentation** link opens the French slides and **Simulation 3D** opens Rabat Flow. Return to the dashboard for the live event stages. [Live dashboard](http://localhost:8050/)

Enter `1`, `20`, `100`, and `500` in the terminal, one at a time. The dashboard highlights the active stage and displays confirmations from Kafka, events written to Redis, and the time for the stage. Once the status reads **Confirmé dans Redis**, use the markers to show where people entered the stadium or validated a transit trip. Each confirmed burst replaces the previous burst’s markers. Enter `q` to close the session.

| Enter | What to show |
| --- | --- |
| `1` | Follow the first event through Kafka and Redis to its marker on the site. |
| `20` | Show how stadium access markers cluster at the gates while transit validations cluster by the stop. |
| `100` | Turn on the H3 overlay and use the legend to compare density across cells. |
| `500` | Show the pressure alert and suggested shuttle beside the stadium view. |

For an automatic rehearsal, run `./present.sh all`. It pauses for six seconds after each of the first three confirmed stages. Run `./present.sh 500` for a session with only the final burst. The automatic and single-stage modes keep the dashboard open for inspection when run in an interactive terminal. Press Enter to close them. To present only the slides and standalone 3D simulation, run `./present.sh pitch`. [Standalone simulation](http://localhost:8080/)

## Stadium view and density overlay

The stadium view uses the bundled Three.js assets. Turn on the optional H3 overlay to see rolling event density at resolution 10. The overlay counts events per cell and source over the current time window.

For the optional Mapbox view, set `MAPBOX_ACCESS_TOKEN` in `.env` to an active public `pk.` token with `styles:read` and `fonts:read`, then run `./present.sh mapbox`. For a URL-restricted token, allow the dashboard URL, which defaults to `http://localhost:8050/`. If you change the port, use the same `DEMO_DASH_PORT` for the check and the demo. [Mapbox token management](https://docs.mapbox.com/accounts/guides/tokens/)

## Implementation

| Area | Behavior |
| --- | --- |
| Bash runner | `present.sh` starts a dedicated Compose project on loopback ports, creates Kafka topics, and gives Spark fresh checkpoints. It waits for Spark’s initial offsets and a successful `/ws` readiness probe, then confirms a synthetic hotel snapshot in Redis before offering the first stage. On exit it stops the processes and removes the session’s containers and Redis volume. |
| Service versions | Compose pins `apache/kafka:4.3.1` and `redis:8.8.3-alpine`. `./present.sh upgrade` pulls those images and syncs the Python lockfile, which includes PySpark 4.0.1 and the FastAPI server dependencies. The demo uses four local Spark threads and a two-second streaming trigger. |
| Event confirmations | `worldcup/presentation.py` publishes the requested burst and waits for Kafka acknowledgements. Spark records the processed count in Redis after writing the aggregates. When the counter increase matches the request, the runner saves the coordinates and marks the stage complete. FastAPI watches the session files and sends the stage status and latest passages over `/ws`. |
| Projected dashboard | FastAPI pushes indicators over `/ws` when Redis Pub/Sub signals a committed crowd batch or an accepted hotel update. The active stage, Kafka and Redis ratios, and elapsed time appear at the top. On wide screens the alert and shuttle panels sit beside the stadium view. On narrow screens they stack below it. |
| Density and pressure | The density overlay and venue cards use rolling event-time windows. The modeled stadium and transit stop have capacities of 250 and 340 events per minute respectively, so the 500-event stage can trigger a visible pressure alert. |

Each session saves its result files and service logs under `artifacts/presentation/run.*/`. The terminal prints the exact path when the session closes. For a complete four-stage rehearsal, check that the `stage-*.json` summaries contain `status` set to `complete` and matching `requested`, `published`, and `processed` counts for 1, 20, 100, and 500. The separate `passages.json` file stores only the latest confirmed burst’s synthetic coordinates and source categories.
