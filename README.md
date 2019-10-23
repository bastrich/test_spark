Class names (both main and test source sets) corresponds to task numbers.

1. I never used sbt before, so here I just used IDEA generated sbt-project structure
2. In Task 2.2 I've made the following assumptions/suggestions:
   1. I suggested session expiration equal to 12 hours, because I think that if user doesn't do anything within 12 hours, he leaved a site and we shouldn't consider him as "user spending time on site".
   2. I suggested that user can appear and can be counted in 2 segments if he visited site with interval more than 12 hours
3. In Task 2.3 I've made the following assumptions/suggestions:
   1. I didn't use session expiration
   2. I assume that product view start with first event with given category, product and user, and ends with last event with given category, product and user over the whole dataset.

My suggestions/assumptions are based on absence of full requirements, and I can, for example, remove 12h-expiration from Task 2.2, or add session expiration to Task 2.3, or make other changes if needed, and if more detailed information will be provided.