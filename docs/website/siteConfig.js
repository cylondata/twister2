/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

// List of projects/orgs using your project for the users page.
const users = [
    {
        caption: 'User1',
        // You will need to prepend the image path with your baseUrl
        // if it is not '/', like: '/test-site/img/image.jpg'.
        image: '/img/undraw_open_source.svg',
        infoLink: 'https://www.facebook.com',
        pinned: true,
    },
];

const baseUrl = '/';

const siteConfig = {
    title: 'Twister2', // Title for your website.
    tagline: 'High Performance Data Analytics',
    url: 'https://twister2.org', // Your website URL
    baseUrl: '/', // Base URL for your project */
    cname: 'twister2.org',
    // For github.io type URLs, you would set the url and baseUrl like:
    //   url: 'https://facebook.github.io',
    //   baseUrl: '/test-site/',

    // Used for publishing and more
    projectName: 'twister2',
    organizationName: 'dsc-spidal',
    // For top-level user or org sites, the organization is still the same.
    // e.g., for the   https://JoelMarcey.github.io site, it would be set like...
    //   organizationName: 'JoelMarcey'

    // For no header links in the top nav bar -> headerLinks: [],
    headerLinks: [
        {doc: 'introduction', label: 'Getting Started'},
        {doc: 'compiling/compile_overview', label: 'Docs'},
        {doc: 'examples/tset/hello_world', label: 'Tutorial'},
        {doc: 'ai/artificial_intelligence', label: 'AI'},
        {doc: 'examples/examples', label: 'Examples'},
        {doc: 'developers/debugging', label: 'Contribute'},
        {doc: 'download', label: 'Download'},
        {page: 'configs', label: 'Configurations'},
        {page: 'javadocs/index.html', label: "Java Docs"},
        {href: "https://github.com/DSC-SPIDAL/twister2", label: "GitHub", external: true},
        {blog: true, label: 'Blog'},
    ],

    // If you have users set above, you add it here:
    users: [],

    /* path to images for header/footer */
    headerIcon: 'img/logo_large.png',
    footerIcon: 'img/logo_large.png',
    favicon: 'img/favicon.ico',

    /* Colors for website */
    colors: {
        primaryColor: '#1c1b8b',
        secondaryColor: '#625f76',
    },

    separateCss: [
        `javadocs`
    ],

    /* Custom fonts for website */
    /*
    fonts: {
      myFont: [
        "Times New Roman",
        "Serif"
      ],
      myOtherFont: [
        "-apple-system",
        "system-ui"
      ]
    },
    */

    // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
    copyright: `Copyright © ${new Date().getFullYear()} Indiana University`,

    highlight: {
        // Highlight.js theme to use for syntax highlighting in code blocks.
        theme: 'atom-one-dark',
    },

    // Add custom scripts here that would be placed in <script> tags.
    // Add custom scripts here that would be placed in <script> tags
    scripts: [
        'https://buttons.github.io/buttons.js',
        'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.0/clipboard.min.js',
        `${baseUrl}js/code-blocks-buttons.js`
    ],
    stylesheets: [
        `${baseUrl}css/code-blocks-buttons.css`,
        `https://fonts.googleapis.com/css?family=Montserrat|Oswald|Roboto&display=swap`
    ],

    // On page navigation for the current documentation page.
    onPageNav: 'separate',
    // No .html extensions for paths.
    cleanUrl: true,

    disableHeaderTitle: false,

    // Open Graph and Twitter card images.
    ogImage: 'img/undraw_online.svg',
    twitterImage: 'img/undraw_tweetstorm.svg',

    projectDescription: `
    Twister2 is a high performance data analytics platform for data pipelines, analytics and streaming
  `,


    // Show documentation's last contributor's name.
    // enableUpdateBy: true,

    // Show documentation's last update time.
    // enableUpdateTime: true,

    // You may provide arbitrary config keys to be used as needed by your
    // template. For example, if you need your repo's URL...
    //   repoUrl: 'https://github.com/facebook/test-site',
};

module.exports = siteConfig;
