# Contributing to Twister2

## Contributing Code Changes

Please review the preceding section before proposing a code change. This section documents how to do so.

When you contribute code, you affirm that the contribution is your original work and that you license the work to the project under the project’s open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project’s open source license and warrant that you have the legal authority to do so.

## Pull Request

1. Fork the Github repository at https://github.com/DSC-SPIDAL/twister2 
2. Clone your fork, create a new branch, push commits to the branch.
3. Consider whether documentation or tests need to be added or updated as part of the change, and add them as needed.
4. Build code with `bazel build bazel build --config=ubuntu twister2/...` If style checks fail, review the code style.
5. Open a pull request against the master branch of twister2. 
   * The PR title should be of the form [Twister2-xxxx][COMPONENT] Title, where TWISTER2-xxxx is the relevant Issue number, COMPONENT the twister2 module. 
   * If the pull request is still a work in progress, and so is not ready to be merged, but needs to be pushed to Github to facilitate review, then add [WIP] after the component.
   * Consider identifying committers or other contributors who have worked on the code being changed. Find the file(s) in Github and click “Blame” to see a line-by-line annotation of who changed the code last. You can add @username in the PR description to ping them immediately.
   * Please state that the contribution is your original work and that you license the work to the project under the project’s open source license.
5. Travis will build and test your changes
   * After about 1 hour, Travis will post the results of the test to the pull request, along with a link to the full results on Travis.
   * Watch for the results, and investigate and fix failures promptly
6. Fixes can simply be pushed to the same branch from which you opened your pull request
7. Travis will automatically re-test when new commits are pushed

## The Review Process

1. Other reviewers, including project members, may comment on the changes and suggest modifications. Changes can be added by simply pushing more commits to the same branch.
2. Lively, polite, rapid technical debate is encouraged from everyone in the community. The outcome may be a rejection of the entire change.
4. Keep in mind that changes to more critical parts of Twister2, like its communication, task execution, will be subjected to more review, and may require more testing and proof of its correctness than other changes.
5. Reviewers can indicate that a change looks suitable for merging with a comment such as: “I think this patch looks good”. 
6. Sometimes, other changes will be merged which conflict with your pull request’s changes. The PR can’t be merged until the conflict is resolved. 
7. Try to be responsive to the discussion rather than let days pass between replies

