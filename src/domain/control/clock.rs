use std::time::Duration;
use tokio::time::Instant;

/// 许可同时保存包含休眠的系统计数与 I/O 截止点，墙钟变化不能延长控制权。
#[derive(Clone, Copy)]
pub struct LeaseTime {
    /// 创建证据时包含系统休眠的本地连续纳秒计数，用于识别暂停跨越寿命。
    started: u128,
    /// Tokio 单调截止点，实际可用期限还需与连续时钟已耗时间共同收窄。
    deadline: Instant,
    /// 从原始请求起点计算的固定有限寿命，不因轮询或回复迟到重新计时。
    ttl: Duration,
}

impl LeaseTime {
    /// 业务作用：在请求发送之前建立有限控制期限，网络往返和调度等待消耗同一预算。
    /// 参数说明：
    /// - `ttl`：权威合同允许的最长许可寿命。
    ///
    /// 返回：固定期限；不具备连续时钟的平台或时钟读取失败时拒绝取得许可。
    pub fn start(ttl: Duration) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !ttl.is_zero() && ttl <= Duration::from_secs(300),
            "invalid_lease_ttl"
        );
        Ok(Self {
            started: ticks()?,
            deadline: Instant::now() + ttl,
            ttl,
        })
    }

    /// 业务作用：合并必须同时有效的控制证据，派生期限不能越过任一原始时钟边界。
    /// 参数说明：
    /// - `other`：同一主机上需要共同约束业务准入的有限证据。
    ///
    /// 返回：两个计时来源分别取交集的证据；没有剩余交集时保持失效，不重新起算寿命。
    pub fn intersect(self, other: Self) -> Self {
        // 保留较晚的原始起点，使任一证据遇到计数回退时仍拒绝准入；相应扣除另一证据已消耗的预算。
        let started = self.started.max(other.started);
        let remaining = self
            .ttl
            .as_nanos()
            .saturating_sub(started - self.started)
            .min(other.ttl.as_nanos().saturating_sub(started - other.started));
        Self {
            started,
            deadline: self.deadline.min(other.deadline),
            ttl: Duration::from_nanos(u64::try_from(remaining).unwrap_or_default()),
        }
    }

    /// 业务作用：在业务准入点同步核对休眠和单调计时，不依赖后台到期任务是否已调度。
    /// 参数说明: 无。
    ///
    /// 返回：剩余有效期限；计数回退、读取失败或任何时钟已到期都返回 None。
    pub fn deadline(&self) -> Option<Instant> {
        let elapsed = ticks().ok()?.checked_sub(self.started)?;
        let remaining = self.ttl.as_nanos().checked_sub(elapsed)?;
        if remaining == 0 || self.deadline <= Instant::now() {
            return None;
        }
        let remaining = Duration::from_nanos(u64::try_from(remaining).ok()?);
        Some(self.deadline.min(Instant::now() + remaining))
    }

    /// 业务作用：证明观察之后已经经过包含系统休眠的完整等待窗口，不能把墙钟跳变当作失效证据。
    /// 参数说明: 无。
    ///
    /// 返回：两个计时来源均已超过窗口时为 true，读取异常保持未完成。
    pub fn elapsed(&self) -> bool {
        ticks()
            .ok()
            .and_then(|now| now.checked_sub(self.started))
            .is_some_and(|elapsed| elapsed >= self.ttl.as_nanos())
            && Instant::now() >= self.deadline
    }
}

/// 业务作用：读取当前主机覆盖休眠的纳秒计数，用于有限许可和失效观察。
/// 参数说明: 无。
///
/// 返回：只在当前主机内比较的非墙钟计数；不支持的平台保持保护。
fn ticks() -> anyhow::Result<u128> {
    #[cfg(target_os = "linux")]
    {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // 内核只写入本地完整 timespec；CLOCK_BOOTTIME 包含 suspend，不受人工校时影响。
        let status = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut time) };
        anyhow::ensure!(
            status == 0 && time.tv_sec >= 0 && time.tv_nsec >= 0,
            "lease_clock_unavailable"
        );
        Ok(time.tv_sec as u128 * 1_000_000_000 + time.tv_nsec as u128)
    }
    #[cfg(target_os = "macos")]
    {
        #[repr(C)]
        struct Timebase {
            /// 系统连续时钟刻度换算到纳秒的分子，由系统接口提供。
            numer: u32,
            /// 系统时钟换算比例的非零分母，避免把硬件刻度误当成时间单位。
            denom: u32,
        }
        unsafe extern "C" {
            /// 业务作用：取得包含主机休眠的连续计数，使休眠期间已经耗尽的许可不能继续写出。
            /// 参数说明: 无。
            ///
            /// 返回：当前主机内的无符号硬件刻度；须结合 timebase 换算，不可作为墙钟或跨主机时间。
            fn mach_continuous_time() -> u64;
            /// 业务作用：取得连续计数到纳秒的比例，保证许可寿命使用系统定义的时间单位。
            /// 参数说明：
            /// - `info`：调用方持有的可写完整 Timebase 指针；系统仅在该固定 C 布局内写入比例字段。
            ///
            /// 返回：零表示系统调用成功；调用方仍须检查分子和分母非零，失败时不签发许可。
            fn mach_timebase_info(info: *mut Timebase) -> i32;
        }
        let mut timebase = Timebase {
            numer: 0,
            denom: 0,
        };
        // 系统 API 写入固定布局的两个 u32；连续计数包含机器休眠，换算使用 u128 避免溢出。
        let status = unsafe { mach_timebase_info(&mut timebase) };
        anyhow::ensure!(
            status == 0 && timebase.numer > 0 && timebase.denom > 0,
            "lease_clock_unavailable"
        );
        let value = unsafe { mach_continuous_time() };
        Ok(value as u128 * timebase.numer as u128 / timebase.denom as u128)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    anyhow::bail!("continuous_lease_clock_unsupported")
}
