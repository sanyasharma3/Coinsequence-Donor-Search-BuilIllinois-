# Coinsequence-Donor-Search-BuilIllinois-
## Overview
Coinsequence is dedicated to creating meaningful connections between donors and students, particularly those who are navigating through the complex landscape of student debt. In our latest feature update, we focus on enhancing the matchmaking capabilities of our platform to ensure that donors can find and support students whose experiences and backgrounds resonate with them personally. This update is especially pivotal for our mission to support underrepresented groups, such as women in STEM from the Chicago area.

## Technical Challenges
Our data infrastructure is composed of various datatables and microservices, which adds a level of complexity to the task of matchmaking. The data pertinent to the lives and stories of students is distributed and not centralized, making it a challenging task to create a seamless search experience.

Despite these challenges, we have opted to refine our database querying methods instead of relying on third-party search engines like ElasticSearch or Algolia. We believe this approach not only aligns with our commitment to data security and privacy but also allows for deeper integration and customization that reflects the unique needs of our community.

## The Matchmaking Capability
With this feature, donors can perform targeted searches — for instance, looking specifically for "women in STEM from the Chicago area." Despite the apparent simplicity of such a search, the power behind this functionality lies in its ability to navigate through multiple streams of data to retrieve the most relevant student profiles.

This matchmaking capability is not trivial; it is a sophisticated solution to the data distribution problem. It enables our platform to:

- Provide accurate and relevant search results by intelligently querying distributed databases.
- Create personalized connections based on specific donor preferences and criteria.
- Showcase student profiles in a way that highlights their individual stories and educational journeys.

## Implementation in the Hackathon
During the hackathon, we are concentrated on developing this feature to be as intuitive and powerful as possible. The goal is to create a tool that not only functions effectively on a technical level but also facilitates genuine connections that can lead to sustained support for students.

## Code
`profile.py` contains some sample code representing the student profile, since we are unable to provide full access to our code repository.
