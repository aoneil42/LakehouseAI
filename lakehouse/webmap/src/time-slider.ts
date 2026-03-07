/**
 * Time slider component for temporal dataset filtering.
 * Provides dual-range slider with play/pause/step controls.
 */

export interface TimeConfig {
  column: string;
  min: Date;
  max: Date;
  distinctCount: number;
}

export type TimeChangeCallback = (start: Date, end: Date) => void;

export class TimeSlider {
  private container: HTMLDivElement;
  private windowStart: Date;
  private windowEnd: Date;
  private playing = false;
  private playInterval: ReturnType<typeof setInterval> | null = null;
  private stepMs: number;

  constructor(
    private config: TimeConfig,
    private onChange: TimeChangeCallback,
    private onClose?: () => void
  ) {
    this.windowStart = config.min;
    this.windowEnd = config.max;

    const rangeMs = config.max.getTime() - config.min.getTime();
    const targetSteps = Math.min(200, config.distinctCount);
    // Don't let the step exceed the total range
    this.stepMs = Math.min(
      Math.max(rangeMs / targetSteps, 60000), // at least 1 minute
      rangeMs // but never more than the full range
    );

    // Pick the best default window size for the data range
    const defaultWindow =
      rangeMs <= 3600000 ? "hour" :       // ≤ 1 hour
      rangeMs <= 86400000 ? "day" :        // ≤ 1 day
      rangeMs <= 604800000 ? "week" :      // ≤ 1 week
      rangeMs <= 2592000000 ? "month" :    // ≤ 30 days
      "year";

    this.container = document.createElement("div");
    this.container.className = "time-slider-panel";
    this.container.innerHTML = `
      <div class="time-slider-header">
        <span class="time-slider-label">${config.column}</span>
        <span class="time-slider-range" id="time-range-label">
          ${this.fmt(config.min)} &mdash; ${this.fmt(config.max)}
        </span>
        <button class="time-close-btn" title="Disable time filter">&#x2715;</button>
      </div>
      <div class="time-slider-controls">
        <button class="time-btn time-step-back" title="Step back">&#x23EE;</button>
        <button class="time-btn time-play" title="Play/Pause">&#x25B6;</button>
        <button class="time-btn time-step-fwd" title="Step forward">&#x23ED;</button>
        <div class="time-slider-track">
          <input type="range" class="time-range-input time-range-start" min="0" max="1000" value="0" />
          <input type="range" class="time-range-input time-range-end" min="0" max="1000" value="1000" />
        </div>
        <select class="time-window-select" title="Time window width">
          <option value="hour"${defaultWindow === "hour" ? " selected" : ""}>Hourly</option>
          <option value="day"${defaultWindow === "day" ? " selected" : ""}>Daily</option>
          <option value="week"${defaultWindow === "week" ? " selected" : ""}>Weekly</option>
          <option value="month"${defaultWindow === "month" ? " selected" : ""}>Monthly</option>
          <option value="year"${defaultWindow === "year" ? " selected" : ""}>Yearly</option>
          <option value="all">Full Range</option>
        </select>
        <select class="time-speed-select" title="Playback speed">
          <option value="2000">0.5x</option>
          <option value="1000" selected>1x</option>
          <option value="500">2x</option>
          <option value="250">4x</option>
        </select>
      </div>
    `;
  }

  mount(parent: HTMLElement): void {
    parent.appendChild(this.container);
    this.wireEvents();
  }

  destroy(): void {
    this.stopPlay();
    this.container.remove();
  }

  private wireEvents(): void {
    const playBtn = this.container.querySelector(".time-play")!;
    const stepBack = this.container.querySelector(".time-step-back")!;
    const stepFwd = this.container.querySelector(".time-step-fwd")!;
    const closeBtn = this.container.querySelector(".time-close-btn")!;
    const startSlider = this.container.querySelector(
      ".time-range-start"
    ) as HTMLInputElement;
    const endSlider = this.container.querySelector(
      ".time-range-end"
    ) as HTMLInputElement;
    const windowSelect = this.container.querySelector(
      ".time-window-select"
    ) as HTMLSelectElement;
    const speedSelect = this.container.querySelector(
      ".time-speed-select"
    ) as HTMLSelectElement;

    playBtn.addEventListener("click", () => this.togglePlay());
    stepBack.addEventListener("click", () => this.step(-1));
    stepFwd.addEventListener("click", () => this.step(1));
    closeBtn.addEventListener("click", () => {
      this.destroy();
      this.onClose?.();
    });

    const rangeMs = this.config.max.getTime() - this.config.min.getTime();

    startSlider.addEventListener("input", () => {
      const pct = parseInt(startSlider.value) / 1000;
      this.windowStart = new Date(this.config.min.getTime() + pct * rangeMs);
      if (this.windowStart >= this.windowEnd) {
        this.windowStart = new Date(this.windowEnd.getTime() - this.stepMs);
      }
      this.updateLabel();
      this.emitChange();
    });

    endSlider.addEventListener("input", () => {
      const pct = parseInt(endSlider.value) / 1000;
      this.windowEnd = new Date(this.config.min.getTime() + pct * rangeMs);
      if (this.windowEnd <= this.windowStart) {
        this.windowEnd = new Date(this.windowStart.getTime() + this.stepMs);
      }
      this.updateLabel();
      this.emitChange();
    });

    const WINDOW_MS: Record<string, number> = {
      hour: 3600000,
      day: 86400000,
      week: 604800000,
      month: 2592000000,
      year: 31536000000,
    };

    windowSelect.addEventListener("change", () => {
      const preset = windowSelect.value;
      if (preset === "all") {
        this.stepMs = rangeMs;
      } else {
        // Clamp step to not exceed the full data range
        this.stepMs = Math.min(WINDOW_MS[preset] ?? 86400000, rangeMs);
      }
      this.windowEnd = new Date(this.windowStart.getTime() + this.stepMs);
      this.clamp();
      this.updateSliders();
      this.updateLabel();
      this.emitChange();
    });

    speedSelect.addEventListener("change", () => {
      if (this.playing) {
        this.stopPlay();
        this.startPlay(parseInt(speedSelect.value));
      }
    });
  }

  private togglePlay(): void {
    const btn = this.container.querySelector(".time-play")!;
    if (this.playing) {
      this.stopPlay();
      btn.textContent = "\u25B6";
    } else {
      const speed = parseInt(
        (
          this.container.querySelector(
            ".time-speed-select"
          ) as HTMLSelectElement
        ).value
      );
      this.startPlay(speed);
      btn.textContent = "\u23F8";
    }
  }

  private startPlay(intervalMs: number): void {
    this.playing = true;
    this.playInterval = setInterval(() => {
      this.step(1);
      if (this.windowEnd >= this.config.max) {
        this.stopPlay();
        this.container.querySelector(".time-play")!.textContent = "\u25B6";
      }
    }, intervalMs);
  }

  private stopPlay(): void {
    this.playing = false;
    if (this.playInterval) clearInterval(this.playInterval);
    this.playInterval = null;
  }

  private step(direction: 1 | -1): void {
    const duration = this.windowEnd.getTime() - this.windowStart.getTime();
    this.windowStart = new Date(
      this.windowStart.getTime() + direction * duration * 0.5
    );
    this.windowEnd = new Date(this.windowStart.getTime() + duration);
    this.clamp();
    this.updateSliders();
    this.updateLabel();
    this.emitChange();
  }

  private clamp(): void {
    if (this.windowStart < this.config.min) this.windowStart = this.config.min;
    if (this.windowEnd > this.config.max) this.windowEnd = this.config.max;
  }

  private updateSliders(): void {
    const rangeMs = this.config.max.getTime() - this.config.min.getTime();
    const startPct =
      (this.windowStart.getTime() - this.config.min.getTime()) / rangeMs;
    const endPct =
      (this.windowEnd.getTime() - this.config.min.getTime()) / rangeMs;

    (
      this.container.querySelector(".time-range-start") as HTMLInputElement
    ).value = String(Math.round(startPct * 1000));
    (
      this.container.querySelector(".time-range-end") as HTMLInputElement
    ).value = String(Math.round(endPct * 1000));
  }

  private updateLabel(): void {
    const label = this.container.querySelector("#time-range-label")!;
    label.textContent = `${this.fmt(this.windowStart)} \u2014 ${this.fmt(this.windowEnd)}`;
  }

  private emitChange(): void {
    this.onChange(this.windowStart, this.windowEnd);
  }

  private fmt(d: Date): string {
    return d.toISOString().slice(0, 19).replace("T", " ") + " UTC";
  }
}
