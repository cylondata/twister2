/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

const React = require("react");

const CompLibrary = require("../../core/CompLibrary.js");

const {MarkdownBlock, GridBlock, Container} = CompLibrary; /* Used to read markdown */

const siteConfig = require(`${process.cwd()}/siteConfig.js`);

function docUrl(doc, language) {
    return `${siteConfig.baseUrl}${language ? `${language}/` : ""}${doc}`;
}

function imgUrl(img) {
    return `${siteConfig.baseUrl}img/${img}`;
}

class Button extends React.Component {
    render() {
        return (
            <div className="pluginWrapper buttonWrapper">
            <a className="button hero" href={this.props.href} target={this.props.target}>
        {this.props.children}
    </a>
        </div>
    );
    }
}

Button.defaultProps = {
    target: "_self"
};

const SplashContainer = props => (
<div className="homeContainer">
    <div className="homeSplashFade">
    <div className="wrapper homeWrapper">{props.children}</div>
</div>
</div>
);


const ProjectTitle = () => (
<React.Fragment>
<div style={{display : "flex", justifyContent : "center", alignItems : "center"}}>
<img src={"img/half.jpg"} alt="Twister2 Logo" width={100} height={100}/>
<h1 className="projectTitle">{siteConfig.title}</h1>
</div>

<h2 style={{marginTop : "0.5em"}}>
Flexible, High performance data analytics.
</h2>
</React.Fragment>
);

const PromoSection = props => (
<div className="section promoSection">
    <div className="promoRow">
    <div className="pluginRowBlock">{props.children}</div>
</div>
</div>
);

class HomeSplash extends React.Component {
    render() {
        const language = this.props.language || "";
        return (
            <SplashContainer>
            <div className="inner">
            <ProjectTitle />
            <PromoSection>
            <Button href={docUrl("docs/quickstart", language)}>
        Get Started
        </Button>
        </PromoSection>
        </div>
        </SplashContainer>
    );
    }
}

const Installation = () => (
<div
className="productShowcaseSection"
style={{ textAlign: "center" }}
>
<h2 style={{marginTop : 10, marginBottom : 5}}>Installation</h2>
<MarkdownBlock>
``` npm install --save
      redux ```
</MarkdownBlock>
</div>
);

const Block = props => (
<Container
id={props.id}
background={props.background}
className={props.className}
>
<GridBlock align="center" contents={props.children} layout={props.layout}/>
</Container>
);

const FeaturesTop = props => (
<Block layout="fourColumn" className="rowContainer featureBlock">
    {[
            {
                content: "Offers a set of modular components for building data analytic applications",
                //image: imgUrl('icon/time.png'),
                image : imgUrl("cogs.svg"),
                imageAlign: 'top',
                title: "Flexible"
            },
{
    content: "Work with high performance networks to efficiently execute IO intensive applications",
        image: imgUrl('speed.svg'),
    imageAlign: 'top',
    title: "High Performance"
},
{
    content: `Supports both streaming and batch natively in a single engine.`,
        image: imgUrl('Flow.svg'),
    imageAlign: 'top',
    title: "Straming & Batch"
},
{
    content: "Provides high level APIs similar to Spark, Hadoop and Storm as well as support for BEAM and Storm APIs",
        image: imgUrl('api.svg'),
    imageAlign: 'top',
    title: "API Support"
},
]}
</Block>
);

const Twister2Apps = props => (
<Block layout="twoColumn" className="rowContainer featureBlock">
    {[
            {
                content: "Offers a set of modular components for building data analytic applications",
                //image: imgUrl('icon/time.png'),
                image : imgUrl("cogs.svg"),
                imageAlign: 'top',
                title: "K-Means"
            },
{
    content: "Work with high performance networks to efficiently execute IO intensive applications",
        image: imgUrl('speed.svg'),
    imageAlign: 'top',
    title: "Terasort"
},
]}
</Block>
);

const Community = props => (
<Block layout="twoColumn" className="rowContainer featureBlock">
    {[
            {
                content: "Our goal is to build a community to bridge the gap between high performance computing and data analytics. Join our community and help us to build better and more efficient data analytics products",
                title: "Help Us Improve Twister2!"
            },
]}
</Block>
);

// const Community = () => (
// <Block layout="oneColumn" className="featureBlock rowContainer">
//     {[
//             {
//                 content: "Our goal is to build a community to bridge the gap between high performance computing and data analytics. Join our community and help us to build better and more efficient data analytics producs",
//                 title: "Help Us Improve Twister2!"
//             },
// ]}
// </Block>
// );

class Index extends React.Component {
    render() {
        const language = this.props.language || "";

        return (
            <div>
            <HomeSplash language={language} />
        <div className="mainContainer">

            <div className="productShowcaseSection">
            <Container background="light">
            <FeaturesTop />
            </Container>
            <Container className="libsContainer" wrapper={false}>

            <Twister2Apps/>
            </Container>
            <Container background="light">
            <Community/>
            </Container>
            </div>
            </div>
            </div>
    );
    }
}

module.exports = Index;
