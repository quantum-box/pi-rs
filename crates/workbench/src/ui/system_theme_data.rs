// Recipe constants from Pi v1.5.0 system-theme.ts.
// Upstream copyright and license: `system_theme_pi_license.txt`.
use super::system_theme::{Curve, Family};
pub(super) const FAMILIES: &[(&str, Family)] = &[
    (
        "neutral",
        Family {
            hue: 231.49,
            min: 0.02,
            max: 0.08,
            slot: 8,
        },
    ),
    (
        "blue",
        Family {
            hue: 231.49,
            min: 0.1,
            max: 0.68,
            slot: 4,
        },
    ),
    (
        "green",
        Family {
            hue: 158.68,
            min: 0.1,
            max: 0.76,
            slot: 2,
        },
    ),
    (
        "red",
        Family {
            hue: 20.0,
            min: 0.1,
            max: 0.92,
            slot: 1,
        },
    ),
    (
        "yellow",
        Family {
            hue: 82.36,
            min: 0.5,
            max: 1.0,
            slot: 3,
        },
    ),
    (
        "orange",
        Family {
            hue: 52.0,
            min: 0.12,
            max: 0.85,
            slot: 3,
        },
    ),
    (
        "violet",
        Family {
            hue: 295.0,
            min: 0.2,
            max: 0.6,
            slot: 5,
        },
    ),
    (
        "calamine",
        Family {
            hue: 202.43,
            min: 0.1,
            max: 0.74,
            slot: 6,
        },
    ),
    (
        "thinkingSlate",
        Family {
            hue: 231.49,
            min: 0.08,
            max: 0.2,
            slot: 4,
        },
    ),
    (
        "thinkingBlue",
        Family {
            hue: 231.49,
            min: 0.2,
            max: 0.45,
            slot: 4,
        },
    ),
    (
        "thinkingPeriwinkle",
        Family {
            hue: 263.25,
            min: 0.3,
            max: 0.6,
            slot: 6,
        },
    ),
    (
        "thinkingViolet",
        Family {
            hue: 295.0,
            min: 0.4,
            max: 0.75,
            slot: 5,
        },
    ),
    (
        "thinkingMagenta",
        Family {
            hue: 337.5,
            min: 0.5,
            max: 0.85,
            slot: 13,
        },
    ),
    (
        "thinkingRed",
        Family {
            hue: 20.0,
            min: 0.95,
            max: 1.0,
            slot: 1,
        },
    ),
];
pub(super) const TOKENS: &[(&str, &str)] = &[
    ("selectedBg", "blue"),
    ("searchMatchBg", "orange"),
    ("userMessageBg", "blue"),
    ("customMessageBg", "violet"),
    ("toolPendingBg", "neutral"),
    ("toolSuccessBg", "green"),
    ("toolErrorBg", "red"),
    ("text", "neutral"),
    ("userMessageText", "neutral"),
    ("customMessageText", "neutral"),
    ("toolTitle", "neutral"),
    ("syntaxOperator", "neutral"),
    ("syntaxPunctuation", "neutral"),
    ("muted", "neutral"),
    ("dim", "neutral"),
    ("thinkingText", "neutral"),
    ("toolOutput", "neutral"),
    ("mdLinkUrl", "neutral"),
    ("mdQuote", "neutral"),
    ("mdQuoteBorder", "neutral"),
    ("mdHr", "neutral"),
    ("mdCodeBlockBorder", "neutral"),
    ("toolDiffContext", "neutral"),
    ("syntaxComment", "neutral"),
    ("scrollbarTrack", "neutral"),
    ("scrollbarThumb", "neutral"),
    ("searchMatchText", "neutral"),
    ("borderMuted", "neutral"),
    ("accent", "violet"),
    ("borderAccent", "violet"),
    ("customMessageLabel", "violet"),
    ("mdCode", "violet"),
    ("mdListBullet", "violet"),
    ("syntaxType", "violet"),
    ("border", "blue"),
    ("mdLink", "blue"),
    ("syntaxKeyword", "blue"),
    ("syntaxVariable", "calamine"),
    ("success", "green"),
    ("mdCodeBlock", "green"),
    ("toolDiffAdded", "green"),
    ("bashMode", "green"),
    ("syntaxNumber", "green"),
    ("error", "red"),
    ("toolDiffRemoved", "red"),
    ("warning", "yellow"),
    ("mdHeading", "yellow"),
    ("syntaxFunction", "yellow"),
    ("syntaxString", "orange"),
    ("thinkingOff", "neutral"),
    ("thinkingMinimal", "thinkingSlate"),
    ("thinkingLow", "thinkingBlue"),
    ("thinkingMedium", "thinkingPeriwinkle"),
    ("thinkingHigh", "thinkingViolet"),
    ("thinkingXhigh", "thinkingMagenta"),
    ("thinkingMax", "thinkingRed"),
];
pub(super) const LEVELS: &[(&str, [Curve; 2])] = &[
    (
        "panel",
        [
            Curve {
                coefficients: [0.29131, -0.39746, 2.33185, -0.85524, -1.2076, 0.86276],
                reachable: [0.0, 0.979],
            },
            Curve {
                coefficients: [-3.74073, 27.94549, -78.44258, 112.6798, -79.60015, 22.11277],
                reachable: [0.348, 1.0],
            },
        ],
    ),
    (
        "track",
        [
            Curve {
                coefficients: [0.39028, -0.23015, 0.83573, 2.43829, -4.38292, 2.01582],
                reachable: [0.0, 0.946],
            },
            Curve {
                coefficients: [
                    -5.24921, 38.37322, -107.28833, 152.10005, -106.17127, 29.18061,
                ],
                reachable: [0.368, 1.0],
            },
        ],
    ),
    (
        "thinking0",
        [
            Curve {
                coefficients: [0.52988, -0.05809, -0.30924, 4.63567, -6.52933, 2.89108],
                reachable: [0.0, 0.873],
            },
            Curve {
                coefficients: [
                    -28.27749, 182.85284, -469.62416, 603.15916, -384.59976, 97.35147,
                ],
                reachable: [0.51, 1.0],
            },
        ],
    ),
    (
        "thinking1",
        [
            Curve {
                coefficients: [0.55278, -0.03667, -0.45659, 4.95347, -6.90265, 3.0706],
                reachable: [0.0, 0.858],
            },
            Curve {
                coefficients: [
                    -37.10484, 235.86282, -596.62344, 754.3633, -474.00763, 118.3551,
                ],
                reachable: [0.535, 1.0],
            },
        ],
    ),
    (
        "thinking2",
        [
            Curve {
                coefficients: [0.57486, -0.01765, -0.58987, 5.25227, -7.27175, 3.25532],
                reachable: [0.0, 0.842],
            },
            Curve {
                coefficients: [
                    -59.89653, 377.05024, -945.07843, 1182.03145, -734.96375, 181.68658,
                ],
                reachable: [0.556, 1.0],
            },
        ],
    ),
    (
        "thinking3",
        [
            Curve {
                coefficients: [0.59621, -0.00062, -0.71148, 5.53588, -7.6392, 3.44606],
                reachable: [0.0, 0.827],
            },
            Curve {
                coefficients: [
                    -72.07122,
                    445.84082,
                    -1099.57352,
                    1353.88793,
                    -829.53392,
                    202.26164,
                ],
                reachable: [0.58, 1.0],
            },
        ],
    ),
    (
        "thinking4",
        [
            Curve {
                coefficients: [0.61691, 0.01462, -0.82288, 5.80651, -8.00641, 3.64333],
                reachable: [0.0, 0.811],
            },
            Curve {
                coefficients: [
                    -110.14338,
                    674.21488,
                    -1645.75941,
                    2004.32367,
                    -1215.15899,
                    293.3183,
                ],
                reachable: [0.6, 1.0],
            },
        ],
    ),
    (
        "thinking5",
        [
            Curve {
                coefficients: [0.63702, 0.02826, -0.92498, 6.06465, -8.37246, 3.84651],
                reachable: [0.0, 0.795],
            },
            Curve {
                coefficients: [
                    -175.47701,
                    1063.54495,
                    -2570.70594,
                    3098.80776,
                    -1860.15527,
                    444.76392,
                ],
                reachable: [0.62, 1.0],
            },
        ],
    ),
    (
        "thinking6",
        [
            Curve {
                coefficients: [0.65658, 0.04044, -1.01835, 6.30989, -8.73529, 4.05439],
                reachable: [0.0, 0.779],
            },
            Curve {
                coefficients: [
                    -183.81712,
                    1094.70055,
                    -2602.68539,
                    3088.71276,
                    -1826.91131,
                    430.75931,
                ],
                reachable: [0.643, 1.0],
            },
        ],
    ),
    (
        "subtle",
        [
            Curve {
                coefficients: [0.56762, -0.02475, -0.5383, 5.12628, -7.10931, 3.17324],
                reachable: [0.0, 0.848],
            },
            Curve {
                coefficients: [
                    -232.85459,
                    1376.54473,
                    -3249.11801,
                    3827.91186,
                    -2248.29472,
                    526.55751,
                ],
                reachable: [0.657, 1.0],
            },
        ],
    ),
    (
        "thumb",
        [
            Curve {
                coefficients: [0.60323, 0.00278, -0.73328, 5.57157, -7.68067, 3.46933],
                reachable: [0.0, 0.823],
            },
            Curve {
                coefficients: [
                    -82.89897,
                    511.01355,
                    -1255.98095,
                    1540.76821,
                    -940.68087,
                    228.58523,
                ],
                reachable: [0.586, 1.0],
            },
        ],
    ),
    (
        "readable",
        [
            Curve {
                coefficients: [0.66937, 0.04704, -1.06871, 6.43941, -8.9332, 4.17229],
                reachable: [0.0, 0.77],
            },
            Curve {
                coefficients: [
                    -1554.52576,
                    8733.56817,
                    -19604.93507,
                    21977.72696,
                    -12300.99599,
                    2749.81288,
                ],
                reachable: [0.751, 1.0],
            },
        ],
    ),
    (
        "emphasis",
        [
            Curve {
                coefficients: [0.7303, 0.07695, -1.31626, 7.1681, -10.14436, 4.92846],
                reachable: [0.0, 0.712],
            },
            Curve {
                coefficients: [
                    -4948.31942,
                    26870.91986,
                    -58334.48399,
                    63280.17197,
                    -34298.01053,
                    7430.30146,
                ],
                reachable: [0.811, 1.0],
            },
        ],
    ),
    (
        "textOnPanel",
        [
            Curve {
                coefficients: [0.86713, 0.05232, -0.89428, 4.79014, -5.5432, 1.75023],
                reachable: [0.0, 0.542],
            },
            Curve {
                coefficients: [
                    -8570.89457,
                    43954.60805,
                    -90084.00702,
                    92220.6791,
                    -47152.15802,
                    9632.27113,
                ],
                reachable: [0.867, 1.0],
            },
        ],
    ),
    (
        "text",
        [
            Curve {
                coefficients: [0.89242, 0.02311, -0.44862, 2.34417, -0.06084, -2.63844],
                reachable: [0.0, 0.5],
            },
            Curve {
                coefficients: [
                    -2004.67048,
                    6664.47299,
                    -6060.70202,
                    -1792.61209,
                    5133.82359,
                    -1939.85583,
                ],
                reachable: [0.894, 1.0],
            },
        ],
    ),
];
