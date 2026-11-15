//! OKLab and OKHSL conversions adapted from Pi's `oklab.ts`.
//! Pi's copyright and license are in `system_theme_pi_license.txt`.
//!
//! Copyright (c) 2021 Björn Ottosson, used under the MIT license:
//!
//! Permission is hereby granted, free of charge, to any person obtaining a copy
//! of this software and associated documentation files (the "Software"), to deal
//! in the Software without restriction, including without limitation the rights
//! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
//! copies of the Software, and to permit persons to whom the Software is
//! furnished to do so, subject to the following conditions: The above copyright
//! notice and this permission notice shall be included in all copies or
//! substantial portions of the Software. THE SOFTWARE IS PROVIDED "AS IS",
//! WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
//! THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//! NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
//! FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
//! TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR
//! THE USE OR OTHER DEALINGS IN THE SOFTWARE.
type V = [f64; 3];
type M = [V; 3];
const RGB_LMS: M = [
    [0.4122214694707629, 0.5363325372617349, 0.0514459932675022],
    [0.2119034958178251, 0.6806995506452344, 0.1073969535369405],
    [0.0883024591900564, 0.2817188391361215, 0.6299787016738222],
];
const LMS_LAB: M = [
    [0.210454268309314, 0.793617774702305, -0.0040720430116193],
    [1.9779985324311684, -2.42859224204858, 0.450593709617411],
    [0.0259040424655478, 0.7827717124575296, -0.8086757549230774],
];
const LAB_LMS: M = [
    [1., 0.3963377773761749, 0.2158037573099136],
    [1., -0.1055613458156586, -0.0638541728258133],
    [1., -0.0894841775298119, -1.2914855480194092],
];
// Retain the source constants verbatim to make the upstream conversion auditable.
#[allow(clippy::excessive_precision)]
const LMS_RGB: M = [
    [4.0767416360759583, -3.3077115392580629, 0.2309699031821043],
    [-1.2684379732850315, 2.6097573492876882, -0.341319376002657],
    [-0.0041960761386756, -0.7034186179359362, 1.7076146940746117],
];
const FIT: [([f64; 2], [f64; 5]); 3] = [
    (
        [-1.8817031, -0.80936501],
        [1.19086277, 1.76576728, 0.59662641, 0.75515197, 0.56771245],
    ),
    (
        [1.8144408, -1.19445267],
        [0.73956515, -0.45954404, 0.08285427, 0.12541073, -0.14503204],
    ),
    (
        [0.13110758, 1.81333971],
        [1.35733652, -0.00915799, -1.1513021, -0.50559606, 0.00692167],
    ),
];
fn dot(a: V, b: V) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn mul(m: M, v: V) -> V {
    m.map(|row| dot(row, v))
}
fn linear(v: f64) -> f64 {
    if v <= 0.04045 {
        v / 12.92
    } else {
        ((v + 0.055) / 1.055).powf(2.4)
    }
}
fn encoded(v: f64) -> f64 {
    if v > 0.0031308 {
        1.055 * v.powf(1. / 2.4) - 0.055
    } else {
        12.92 * v
    }
}
pub(super) fn rgb_to_lab(c: [u8; 3]) -> V {
    mul(
        LMS_LAB,
        mul(RGB_LMS, c.map(|x| linear(x as f64 / 255.))).map(f64::cbrt),
    )
}
fn lab_to_linear(v: V) -> V {
    mul(LMS_RGB, mul(LAB_LMS, v).map(|x| x.powi(3)))
}
fn linear_to_rgb(v: V) -> [u8; 3] {
    v.map(|x| (encoded(x).clamp(0., 1.) * 255.).round() as u8)
}
pub(super) fn hsl_lightness(x: f64) -> f64 {
    let k3 = 1.206 / 1.03;
    0.5 * (k3 * x - 0.206 + ((k3 * x - 0.206).powi(2) + 4. * 0.03 * k3 * x).sqrt())
}
fn slopes(a: f64, b: f64) -> V {
    LAB_LMS.map(|r| r[1] * a + r[2] * b)
}
fn max_saturation(a: f64, b: f64) -> f64 {
    let idx = FIT
        .iter()
        .enumerate()
        .find(|(i, (xy, _))| *i == 2 || xy[0] * a + xy[1] * b > 1.)
        .unwrap()
        .0;
    let [k0, k1, k2, k3, k4] = FIT[idx].1;
    let weights = LMS_RGB[idx];
    let s = k0 + k1 * a + k2 * b + k3 * a * a + k4 * a * b;
    let d = slopes(a, b);
    let base = d.map(|x| 1. + s * x);
    let f = dot(weights, base.map(|x| x.powi(3)));
    let f1 = dot(
        weights,
        std::array::from_fn(|i| 3. * d[i] * base[i].powi(2)),
    );
    let f2 = dot(
        weights,
        std::array::from_fn(|i| 6. * d[i].powi(2) * base[i]),
    );
    s - f * f1 / (f1 * f1 - 0.5 * f * f2)
}
fn cusp(a: f64, b: f64) -> [f64; 2] {
    let s = max_saturation(a, b);
    let v = lab_to_linear([1., s * a, s * b]);
    let l = (1. / v.into_iter().fold(f64::NEG_INFINITY, f64::max)).cbrt();
    [l, l * s]
}
fn max_chroma(a: f64, b: f64, l: f64, peak: [f64; 2]) -> f64 {
    let [cl, cc] = peak;
    if l <= cl {
        return cc * l / cl;
    }
    let t = cc * (l - 1.) / (cl - 1.);
    let d = slopes(a, b);
    let lms = d.map(|x| l + t * x);
    let cubes = lms.map(|x| x.powi(3));
    let first = std::array::from_fn(|i| 3. * d[i] * lms[i].powi(2));
    let second = std::array::from_fn(|i| 6. * d[i].powi(2) * lms[i]);
    let steps = LMS_RGB.map(|r| {
        let f = dot(r, cubes) - 1.;
        let f1 = dot(r, first);
        let f2 = dot(r, second);
        let u = f1 / (f1 * f1 - 0.5 * f * f2);
        if u >= 0. { -f * u } else { f64::MAX }
    });
    t + steps.into_iter().fold(f64::INFINITY, f64::min)
}
fn stops(l: f64, a: f64, b: f64) -> V {
    let peak = cusp(a, b);
    let cm = max_chroma(a, b, l, peak);
    let k = cm / (l * (peak[1] / peak[0])).min((1. - l) * (peak[1] / (1. - peak[0])));
    let ms = 0.11516993
        + 1. / (7.4477897
            + 4.1590124 * b
            + a * (-2.19557347
                + 1.75198401 * b
                + a * (-2.13704948 - 10.02301043 * b
                    + a * (-4.24894561 + 5.38770819 * b + 4.69891013 * a))));
    let mt = 0.11239642
        + 1. / (1.6132032 - 0.68124379 * b
            + a * (0.40370612
                + 0.90148123 * b
                + a * (-0.27087943
                    + 0.6122399 * b
                    + a * (0.00299215 - 0.45399568 * b - 0.14661872 * a))));
    let mid = 0.9
        * k
        * (1. / (1. / (l * ms).powi(4) + 1. / ((1. - l) * mt).powi(4)))
            .sqrt()
            .sqrt();
    let c0 = (1. / (1. / (l * 0.4).powi(2) + 1. / ((1. - l) * 0.8).powi(2))).sqrt();
    [c0, mid, cm]
}
pub(super) fn hsl_to_rgb(h: f64, s: f64, l: f64) -> [u8; 3] {
    let lab_l = (l * l + 0.206 * l) / ((1.206 / 1.03) * (l + 0.03));
    let mut v = [lab_l, 0., 0.];
    if lab_l > 0. && lab_l < 1. && s > 0. {
        let angle = h.rem_euclid(360.).to_radians();
        let (b, a) = angle.sin_cos();
        let [c0, mid, max] = stops(lab_l, a, b);
        let c = if s < 0.8 {
            let t = 1.25 * s;
            let k1 = 0.8 * c0;
            t * k1 / (1. - (1. - k1 / mid) * t)
        } else {
            let t = 5. * (s - 0.8);
            let k1 = 0.2 * mid.powi(2) * 1.25_f64.powi(2) / c0;
            mid + t * k1 / (1. - (1. - k1 / (max - mid)) * t)
        };
        v = [lab_l, c * a, c * b];
    }
    linear_to_rgb(lab_to_linear(v))
}
pub(super) fn rgb_to_hsl(c: [u8; 3]) -> [f64; 3] {
    let [l, a, b] = rgb_to_lab(c);
    let chroma = a.hypot(b);
    let light = hsl_lightness(l);
    if chroma < 1e-9 || light <= 0. || light >= 1. {
        return [0., 0., light];
    }
    let hue = (b.atan2(a).to_degrees() + 360.) % 360.;
    let [c0, mid, max] = stops(l, a / chroma, b / chroma);
    let sat = if chroma < mid {
        let k1 = 0.8 * c0;
        0.8 * (chroma / (k1 + (1. - k1 / mid) * chroma))
    } else {
        let k1 = 0.2 * mid.powi(2) * 1.25_f64.powi(2) / c0;
        let off = chroma - mid;
        0.8 + 0.2 * (off / (k1 + (1. - k1 / (max - mid)) * off))
    };
    [hue, sat.clamp(0., 1.), light]
}
pub(super) fn lch_to_rgb(l: f64, c: f64, h: f64) -> [u8; 3] {
    let (s, co) = h.to_radians().sin_cos();
    let at = |c| lab_to_linear([l, c * co, c * s]);
    let ingamut = |v: V| v.into_iter().all(|x| (-1e-7..=1. + 1e-7).contains(&x));
    let direct = at(c);
    if ingamut(direct) {
        return linear_to_rgb(direct);
    }
    let mut v = at(0.);
    let (mut low, mut high) = (0., c);
    for _ in 0..20 {
        let mid = (low + high) / 2.;
        let next = at(mid);
        if ingamut(next) {
            low = mid;
            v = next
        } else {
            high = mid
        }
    }
    linear_to_rgb(v)
}
