/*
 * recipe.c
 *
 *  Created on: Sep 21, 2012
 *      Author: fumin
 */

#include "recipe.h"

Recipe* recipe_new() {
	Recipe *new_recipe = (Recipe*) malloc(sizeof(Recipe));
	new_recipe->chunknum = 0;
	new_recipe->fileindex = 0;
	new_recipe->filesize = 0;
	new_recipe->filename = 0;
	new_recipe->fd = 0;
	return new_recipe;
}

Recipe* recipe_new_full(char * filename) {
	Recipe *new_recipe = (Recipe*) malloc(sizeof(Recipe));
	new_recipe->chunknum = 0;
	new_recipe->fileindex = 0;
	new_recipe->filesize = 0;
	new_recipe->filename = (char*) malloc(strlen(filename) + 1);
	strcpy(new_recipe->filename, filename);
	new_recipe->fd = 0;
	return new_recipe;
}

void recipe_free(Recipe* trash) {
	if (trash->fd) {
		close(trash->fd);
		trash->fd = 0;
	}
	if (trash->filename)
		free(trash->filename);
	free(trash);
}
